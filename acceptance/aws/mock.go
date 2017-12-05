package awsat

import (
	"reflect"
	"testing"
)

type mock interface {
	Calls() map[string]int
	SetInputs(map[string][]interface{})
	SetTesting(*testing.T)
}

type basicMock struct {
	t         *testing.T
	calls     map[string]int
	expInputs map[string][]interface{}
}

func (m *basicMock) addCall(call string) {
	if m.calls == nil {
		m.calls = make(map[string]int)
	}
	m.calls[call]++
}

func (m *basicMock) Calls() map[string]int {
	return m.calls
}

func (m *basicMock) SetTesting(t *testing.T) {
	m.t = t
}

func (m *basicMock) SetInputs(inputs map[string][]interface{}) {
	m.expInputs = inputs
}

func (m *basicMock) verifyInput(call string, got interface{}) {
	m.t.Helper()
	if m.expInputs == nil {
		return
	}
	nbCall := m.calls[call] - 1
	if len(m.expInputs[call]) <= nbCall {
		m.t.Fatalf("call %d of %s: not enough expected input", nbCall+1, call)
	}
	if want := m.expInputs[call][nbCall]; !reflect.DeepEqual(want, got) {
		m.t.Fatalf("got %#v, want %#v", got, want)
	}
}
