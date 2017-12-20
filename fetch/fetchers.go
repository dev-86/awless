package fetch

import (
	"context"
	"fmt"
	"sync"

	"github.com/wallix/awless/cloud"
	"github.com/wallix/awless/graph"
)

type fetchResult struct {
	ResourceType string
	Err          error
	Resources    []*graph.Resource
	Objects      interface{}
}

type Func func(context.Context, cloud.FetchCache) ([]*graph.Resource, interface{}, error)

type Funcs map[string]Func

type fetcher struct {
	cache         *cache
	fetchFuncs    map[string]Func
	resourceTypes []string
}

func NewFetcher(funcs Funcs) cloud.Fetcher {
	ftr := &fetcher{
		fetchFuncs: make(Funcs),
		cache:      newCache(),
	}
	for resType, f := range funcs {
		ftr.resourceTypes = append(ftr.resourceTypes, resType)
		ftr.fetchFuncs[resType] = f
	}
	return ftr
}

func (f *fetcher) Fetch(ctx context.Context) (cloud.GraphAPI, error) {
	results := make(chan fetchResult, len(f.resourceTypes))
	var wg sync.WaitGroup

	for _, resType := range f.resourceTypes {
		wg.Add(1)
		go func(t string, co context.Context) {
			f.fetchResource(co, t, results)
			wg.Done()
		}(resType, ctx)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	gph := graph.NewGraph()

	ferr := new(Error)
	for res := range results {
		if err := res.Err; err != nil {
			ferr.Add(err)
		}
		gph.AddResource(res.Resources...)
	}

	if ferr.Any() {
		return gph, ferr
	}

	return gph, nil
}

func (f *fetcher) FetchByType(ctx context.Context, resourceType string) (cloud.GraphAPI, error) {
	results := make(chan fetchResult)
	defer close(results)

	go f.fetchResource(ctx, resourceType, results)

	gph := graph.NewGraph()
	select {
	case res := <-results:
		if err := res.Err; err != nil {
			return gph, err
		}
		for _, r := range res.Resources {
			gph.AddResource(r)
		}
		return gph, nil
	}
}

func (f *fetcher) Cache() cloud.FetchCache {
	return f.cache
}

func (f *fetcher) fetchResource(ctx context.Context, resourceType string, results chan<- fetchResult) {
	var err error
	var objects interface{}
	resources := make([]*graph.Resource, 0)

	fn, ok := f.fetchFuncs[resourceType]
	if ok {
		resources, objects, err = fn(ctx, f.cache)
	} else {
		err = fmt.Errorf("no fetch func defined for resource type '%s'", resourceType)
	}

	f.cache.Store(fmt.Sprintf("%s_objects", resourceType), objects)

	results <- fetchResult{
		ResourceType: resourceType,
		Err:          err,
		Resources:    resources,
		Objects:      objects,
	}
}

type cache struct {
	mu     sync.RWMutex
	cached map[string]*keyCache
}

func newCache() *cache {
	return &cache{
		cached: make(map[string]*keyCache),
	}
}

type keyCache struct {
	once   sync.Once
	err    error
	result interface{}
}

func (c *cache) Get(key string, funcs ...func() (interface{}, error)) (interface{}, error) {
	c.mu.Lock()
	cache, ok := c.cached[key]
	if !ok {
		cache = &keyCache{}
		c.cached[key] = cache
	}
	c.mu.Unlock()

	if len(funcs) > 0 {
		cache.once.Do(func() {
			cache.result, cache.err = funcs[0]()
		})
	}

	return cache.result, cache.err
}

func (c *cache) Store(key string, val interface{}) {
	c.mu.Lock()
	c.cached[key] = &keyCache{result: val}
	c.mu.Unlock()
}

func (c *cache) Reset() {
	c.mu.Lock()
	c.cached = make(map[string]*keyCache)
	c.mu.Unlock()
}
