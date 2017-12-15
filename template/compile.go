package template

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/wallix/awless/template/env"
	"github.com/wallix/awless/template/internal/ast"
	"github.com/wallix/awless/template/params"
)

type Mode []compileFunc

var (
	TestCompileMode = []compileFunc{
		resolveMetaPass,
		verifyCommandsDefinedPass,
		failOnDeclarationWithNoResultPass,
		processAndValidateParamsPass,
		checkInvalidReferenceDeclarationsPass,
		resolveHolesPass,
		resolveMissingHolesPass,
		resolveAliasPass,
		inlineVariableValuePass,
	}

	NewRunnerCompileMode = append(TestCompileMode,
		failOnUnresolvedHolesPass,
		failOnUnresolvedAliasPass,
		convertParamsPass,
		validateCommandsPass,
		injectCommandsPass,
	)
)

func Compile(tpl *Template, cenv env.Compiling, mode ...Mode) (*Template, env.Compiling, error) {
	var pass *multiPass

	if len(mode) > 0 {
		pass = newMultiPass(mode[0]...)
	} else {
		pass = newMultiPass(NewRunnerCompileMode...)
	}

	return pass.compile(tpl, cenv)
}

type compileFunc func(*Template, env.Compiling) (*Template, env.Compiling, error)

// Leeloo Dallas
type multiPass struct {
	passes []compileFunc
}

func newMultiPass(passes ...compileFunc) *multiPass {
	return &multiPass{passes: passes}
}

func (p *multiPass) compile(tpl *Template, cenv env.Compiling) (newTpl *Template, newEnv env.Compiling, err error) {
	newTpl, newEnv = tpl, cenv
	for _, pass := range p.passes {
		newTpl, newEnv, err = pass(newTpl, newEnv)
		if err != nil {
			return
		}
	}

	return
}

func verifyCommandsDefinedPass(tpl *Template, cenv env.Compiling) (*Template, env.Compiling, error) {
	if cenv.LookupCommandFunc() == nil {
		return tpl, cenv, fmt.Errorf("command lookuper is undefined")
	}

	for _, node := range tpl.CommandNodesIterator() {
		key := fmt.Sprintf("%s%s", node.Action, node.Entity)
		if cmd := cenv.LookupCommandFunc()(key); cmd == nil {
			return tpl, cenv, fmt.Errorf("cannot find command for '%s'", key)
		}
	}
	return tpl, cenv, nil
}

func resolveMetaPass(tpl *Template, cenv env.Compiling) (*Template, env.Compiling, error) {
	if cenv.LookupMetaCommandFunc() == nil {
		return tpl, cenv, nil
	}

	for _, node := range tpl.CommandNodesIterator() {
		meta := cenv.LookupMetaCommandFunc()(node.Action, node.Entity, node.Keys())
		if meta != nil {
			type R interface {
				Resolve(map[string]string) (*Template, error)
			}
			resolv, ok := meta.(R)
			if !ok {
				return tpl, cenv, errors.New("meta command can not be resolved")
			}
			paramsStr := make(map[string]string)
			for k, v := range node.Params {
				paramsStr[k] = v.String()
			}
			resolved, err := resolv.Resolve(paramsStr)
			if err != nil {
				return tpl, cenv, fmt.Errorf("%s %s: resolve meta command: %s", node.Action, node.Entity, err)
			}
			tpl.ReplaceNodeByTemplate(node, resolved)
		}
	}
	return tpl, cenv, nil
}

func failOnDeclarationWithNoResultPass(tpl *Template, cenv env.Compiling) (*Template, env.Compiling, error) {
	failOnDeclarationWithNoResult := func(node *ast.DeclarationNode) error {
		cmdNode, ok := node.Expr.(*ast.CommandNode)
		if !ok {
			return nil
		}
		key := fmt.Sprintf("%s%s", cmdNode.Action, cmdNode.Entity)
		cmd := cenv.LookupCommandFunc()(key)
		if cmd == nil {
			return fmt.Errorf("validate: cannot find command for '%s'", key)
		}
		type ER interface {
			ExtractResult(interface{}) string
		}
		if _, ok := cmd.(ER); !ok {
			return cmdErr(cmdNode, "command does not return a result, cannot assign to a variable")
		}
		return nil
	}

	for _, dcl := range tpl.declarationNodesIterator() {
		if err := failOnDeclarationWithNoResult(dcl); err != nil {
			return tpl, cenv, err
		}
	}
	return tpl, cenv, nil
}

func processAndValidateParamsPass(tpl *Template, cenv env.Compiling) (*Template, env.Compiling, error) {
	normalizeMissingRequiredParamsAsHoleAndValidate := func(node *ast.CommandNode) error {
		key := fmt.Sprintf("%s%s", node.Action, node.Entity)
		cmd := cenv.LookupCommandFunc()(key)
		if cmd == nil {
			return fmt.Errorf("process params: cannot find command for '%s'", key)
		}
		type PR interface {
			Params() params.Rule
		}
		rule, ok := cmd.(PR)
		if !ok {
			return cmdErr(node, "command does not implement param rules")
		}
		missing := rule.Params().Missing(node.Keys())
		for _, e := range missing {
			normalized := fmt.Sprintf("%s.%s", node.Entity, e)
			node.Params[e] = ast.NewHoleValue(normalized)
		}
		if err := params.Validate(rule.Params(), node.Keys()); err != nil {
			return cmdErr(node, err)
		}
		return nil
	}

	err := tpl.visitCommandNodesE(normalizeMissingRequiredParamsAsHoleAndValidate)
	return tpl, cenv, err
}

func convertParamsPass(tpl *Template, cenv env.Compiling) (*Template, env.Compiling, error) {
	convert := func(node *ast.CommandNode) error {
		key := fmt.Sprintf("%s%s", node.Action, node.Entity)
		cmd := cenv.LookupCommandFunc()(key)
		if cmd == nil {
			return fmt.Errorf("convert: cannot find command for '%s'", key)
		}

		type C interface {
			ConvertParams() ([]string, func(values map[string]interface{}) (map[string]interface{}, error))
		}
		if v, ok := cmd.(C); ok {
			keys, convFunc := v.ConvertParams()
			values := make(map[string]interface{})
			params := node.ToDriverParams()
			for _, k := range keys {
				if vv, ok := params[k]; ok {
					values[k] = vv
				}
			}
			converted, err := convFunc(values)
			if err != nil {
				return cmdErr(node, err)
			}
			for _, k := range keys {
				delete(node.Params, k)
			}
			for k, v := range converted {
				node.Params[k] = ast.NewInterfaceValue(v)
			}
		}
		return nil
	}
	err := tpl.visitCommandNodesE(convert)
	return tpl, cenv, err
}

func validateCommandsPass(tpl *Template, cenv env.Compiling) (*Template, env.Compiling, error) {
	var errs []error

	collectValidationErrs := func(node *ast.CommandNode) error {
		key := fmt.Sprintf("%s%s", node.Action, node.Entity)
		cmd := cenv.LookupCommandFunc()(key)
		if cmd == nil {
			return fmt.Errorf("validate: cannot find command for '%s'", key)
		}
		type V interface {
			ValidateCommand(map[string]interface{}, []string) []error
		}
		if v, ok := cmd.(V); ok {
			var refsKey []string
			for k, p := range node.Params {
				if ref, isRef := p.(ast.WithRefs); isRef && len(ref.GetRefs()) > 0 {
					refsKey = append(refsKey, k)
				}
			}
			for _, validErr := range v.ValidateCommand(node.ToDriverParams(), refsKey) {
				errs = append(errs, fmt.Errorf("%s %s: %s", node.Action, node.Entity, validErr.Error()))
			}
		}
		return nil
	}
	if err := tpl.visitCommandNodesE(collectValidationErrs); err != nil {
		return tpl, cenv, err
	}
	switch len(errs) {
	case 0:
		return tpl, cenv, nil
	case 1:
		return tpl, cenv, fmt.Errorf("validation error: %s", errs[0])
	default:
		var errsSrings []string
		for _, err := range errs {
			if err != nil {
				errsSrings = append(errsSrings, err.Error())
			}
		}
		return tpl, cenv, fmt.Errorf("validation errors:\n\t- %s", strings.Join(errsSrings, "\n\t- "))
	}
}

func injectCommandsPass(tpl *Template, cenv env.Compiling) (*Template, env.Compiling, error) {
	for _, node := range tpl.CommandNodesIterator() {
		key := fmt.Sprintf("%s%s", node.Action, node.Entity)
		node.Command = cenv.LookupCommandFunc()(key).(ast.Command)
		if node.Command == nil {
			return tpl, cenv, fmt.Errorf("inject: cannot find command for '%s'", key)
		}
	}
	return tpl, cenv, nil
}

func checkInvalidReferenceDeclarationsPass(tpl *Template, cenv env.Compiling) (*Template, env.Compiling, error) {
	usedRefs := make(map[string]struct{})

	for _, withRef := range tpl.WithRefsIterator() {
		for _, ref := range withRef.GetRefs() {
			usedRefs[ref] = struct{}{}
		}
	}

	knownRefs := make(map[string]bool)

	var each = func(withRef ast.WithRefs) error {
		for _, ref := range withRef.GetRefs() {
			if _, ok := knownRefs[ref]; !ok {
				return fmt.Errorf("using reference '$%s' but '%s' is undefined in template\n", ref, ref)
			}
		}
		return nil
	}

	for _, st := range tpl.Statements {
		switch n := st.Node.(type) {
		case ast.WithRefs:
			if err := each(n); err != nil {
				return tpl, cenv, err
			}
		case *ast.DeclarationNode:
			expr := st.Node.(*ast.DeclarationNode).Expr
			switch nn := expr.(type) {
			case ast.WithRefs:
				if err := each(nn); err != nil {
					return tpl, cenv, err
				}
			}
		}
		if decl, isDecl := st.Node.(*ast.DeclarationNode); isDecl {
			ref := decl.Ident
			if _, ok := knownRefs[ref]; ok {
				return tpl, cenv, fmt.Errorf("using reference '$%s' but '%s' has already been assigned in template\n", ref, ref)
			}
			knownRefs[ref] = true
		}
	}

	return tpl, cenv, nil
}

func inlineVariableValuePass(tpl *Template, cenv env.Compiling) (*Template, env.Compiling, error) {
	newTpl := &Template{ID: tpl.ID, AST: tpl.AST.Clone()}
	newTpl.Statements = []*ast.Statement{}

	for i, st := range tpl.Statements {
		decl, isDecl := st.Node.(*ast.DeclarationNode)
		if isDecl {
			value, isValue := decl.Expr.(*ast.ValueNode)
			if isValue {
				if val := value.Value.Value(); val != nil {
					cenv.(*compileEnv).addResolvedVariables(decl.Ident, val)
				}
				for j := i + 1; j < len(tpl.Statements); j++ {
					expr := extractExpressionNode(tpl.Statements[j])
					if expr != nil {
						if withRef, ok := expr.(ast.WithRefs); ok {
							withRef.ReplaceRef(decl.Ident, value.Value)
						}
					}
				}
				if value.IsResolved() {
					continue
				}
			}
		}
		newTpl.Statements = append(newTpl.Statements, st)
	}
	return newTpl, cenv, nil
}

func resolveHolesPass(tpl *Template, cenv env.Compiling) (*Template, env.Compiling, error) {
	tpl.visitHoles(func(h ast.WithHoles) {
		processed := h.ProcessHoles(cenv.Fillers())
		cenv.(*compileEnv).addToProcessedFillers(processed)
	})

	return tpl, cenv, nil
}

func resolveMissingHolesPass(tpl *Template, cenv env.Compiling) (*Template, env.Compiling, error) {
	uniqueHoles := make(map[string][]string)
	tpl.visitHoles(func(h ast.WithHoles) {
		for k, v := range h.GetHoles() {
			uniqueHoles[k] = nil
			for _, vv := range v {
				if !contains(uniqueHoles[k], vv) {
					uniqueHoles[k] = append(uniqueHoles[k], vv)
				}
			}
		}
	})
	var sortedHoles []string
	for k := range uniqueHoles {
		sortedHoles = append(sortedHoles, k)
	}
	sort.Strings(sortedHoles)
	fillers := make(map[string]interface{})

	for _, k := range sortedHoles {
		if cenv.MissingHolesFunc() != nil {
			actual := cenv.MissingHolesFunc()(k, uniqueHoles[k])
			fillers[k] = actual
		}
	}

	tpl.visitHoles(func(h ast.WithHoles) {
		processed := h.ProcessHoles(fillers)
		cenv.(*compileEnv).addToProcessedFillers(processed)
	})

	return tpl, cenv, nil
}

func resolveAliasPass(tpl *Template, cenv env.Compiling) (*Template, env.Compiling, error) {
	var emptyResolv []string
	resolvAliasFunc := func(entity string, key string) func(string) (string, bool) {
		return func(alias string) (string, bool) {
			if cenv.AliasFunc() == nil {
				return "", false
			}
			actual := cenv.AliasFunc()(entity, key, alias)
			if actual == "" {
				emptyResolv = append(emptyResolv, alias)
				return "", false
			} else {
				cenv.Log().ExtraVerbosef("alias: resolved '%s' to '%s' for key %s", alias, actual, key)
				return actual, true
			}
		}
	}

	for _, expr := range tpl.expressionNodesIterator() {
		switch ee := expr.(type) {
		case *ast.CommandNode:
			for k, v := range ee.Params {
				if vv, ok := v.(ast.WithAlias); ok {
					vv.ResolveAlias(resolvAliasFunc(ee.Entity, k))
				}
			}
		case *ast.ValueNode:
			if vv, ok := ee.Value.(ast.WithAlias); ok {
				vv.ResolveAlias(resolvAliasFunc("", ""))
			}
		}
	}

	if len(emptyResolv) > 0 {
		return tpl, cenv, fmt.Errorf("cannot resolve aliases: %q. Maybe you need to update your local model with `awless sync` ?", emptyResolv)
	}

	return tpl, cenv, nil
}

func failOnUnresolvedHolesPass(tpl *Template, cenv env.Compiling) (*Template, env.Compiling, error) {
	uniqueUnresolved := make(map[string]struct{})
	tpl.visitHoles(func(withHole ast.WithHoles) {
		for hole := range withHole.GetHoles() {
			uniqueUnresolved[hole] = struct{}{}
		}
	})

	var unresolved []string
	for k := range uniqueUnresolved {
		unresolved = append(unresolved, k)
	}

	if len(unresolved) > 0 {
		sort.Strings(unresolved)
		return tpl, cenv, fmt.Errorf("template contains unresolved holes: %v", unresolved)
	}

	return tpl, cenv, nil
}

func failOnUnresolvedAliasPass(tpl *Template, cenv env.Compiling) (*Template, env.Compiling, error) {
	var unresolved []string

	visitAliases := func(withAlias ast.WithAlias) {
		for _, alias := range withAlias.GetAliases() {
			unresolved = append(unresolved, alias)
		}
	}

	for _, n := range tpl.expressionNodesIterator() {
		switch nn := n.(type) {
		case *ast.ValueNode:
			if withAlias, ok := nn.Value.(ast.WithAlias); ok {
				visitAliases(withAlias)
			}
		case *ast.CommandNode:
			for _, param := range nn.Params {
				if withAlias, ok := param.(ast.WithAlias); ok {
					visitAliases(withAlias)
				}
			}
		}
	}

	if len(unresolved) > 0 {
		return tpl, cenv, fmt.Errorf("template contains unresolved alias: %v", unresolved)
	}

	return tpl, cenv, nil
}

func cmdErr(cmd *ast.CommandNode, i interface{}, a ...interface{}) error {
	var prefix string
	if cmd != nil {
		prefix = fmt.Sprintf("%s %s: ", cmd.Action, cmd.Entity)
	}
	var msg string
	switch ii := i.(type) {
	case nil:
		return nil
	case string:
		msg = ii
	case error:
		msg = ii.Error()
	}
	if len(a) == 0 {
		return errors.New(prefix + msg)
	}
	return fmt.Errorf("%s"+msg, append([]interface{}{prefix}, a...)...)
}

func contains(arr []string, s string) bool {
	for _, v := range arr {
		if v == s {
			return true
		}
	}
	return false
}

func (t *Template) ReplaceNodeByTemplate(n ast.Node, tplToReplace *Template) error {
	nodeIndex := -1
	for i, st := range t.Statements {
		if st.Node == n {
			nodeIndex = i
		}
	}
	if nodeIndex == -1 {
		return fmt.Errorf("node '%v' not found", n)
	}
	after := make([]*ast.Statement, len(t.Statements[nodeIndex+1:]))
	copy(after, t.Statements[nodeIndex+1:])
	t.Statements = append(t.Statements[:nodeIndex], tplToReplace.Statements...)
	t.Statements = append(t.Statements, after...)
	return nil
}
