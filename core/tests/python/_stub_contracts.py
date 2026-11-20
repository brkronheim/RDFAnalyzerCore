"""Structural stub drift checks; intentionally not a replacement for type checking."""
import ast
import dataclasses
import importlib
import inspect
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]


def assert_stub_contract(test, name, *, private_classes=()):
    module = importlib.import_module(name)
    tree = ast.parse(Path(module.__file__).with_suffix('.pyi').read_text())
    classes = {node.name: node for node in tree.body if isinstance(node, ast.ClassDef)
        and (not node.name.startswith('_') or node.name in private_classes)}
    runtime = {key: value for key, value in vars(module).items()
        if inspect.isclass(value) and value.__module__ == name
        and (not key.startswith('_') or key in private_classes)}
    test.assertEqual(set(classes), set(runtime))

    def signature(obj, node):
        parameters = list(inspect.signature(obj).parameters.values())
        ordinary = node.args.posonlyargs+node.args.args
        defaults = {arg.arg for arg in ordinary[len(ordinary)-len(node.args.defaults):]}
        stub_parameters = [(arg.arg, inspect.Parameter.POSITIONAL_ONLY) for arg in node.args.posonlyargs]
        stub_parameters += [(arg.arg, inspect.Parameter.POSITIONAL_OR_KEYWORD) for arg in node.args.args]
        if node.args.vararg:
            stub_parameters.append((node.args.vararg.arg, inspect.Parameter.VAR_POSITIONAL))
        stub_parameters += [(arg.arg, inspect.Parameter.KEYWORD_ONLY) for arg in node.args.kwonlyargs]
        if node.args.kwarg:
            stub_parameters.append((node.args.kwarg.arg, inspect.Parameter.VAR_KEYWORD))
        defaults.update(arg.arg for arg, value in zip(node.args.kwonlyargs, node.args.kw_defaults)
            if value is not None)
        test.assertEqual(stub_parameters, [(param.name, param.kind) for param in parameters], (name, node.name))
        test.assertEqual(defaults, {param.name for param in parameters if param.default is not inspect.Parameter.empty}, (name, node.name))
        test.assertIsNotNone(node.returns, (name, node.name))

    for key, cls in runtime.items():
        stub = classes[key]
        if dataclasses.is_dataclass(cls):
            fields = [node for node in stub.body if isinstance(node, ast.AnnAssign)]
            test.assertEqual([node.target.id for node in fields],
                [field.name for field in dataclasses.fields(cls)])
            optional = {field.name for field in dataclasses.fields(cls)
                if field.default is not dataclasses.MISSING or field.default_factory is not dataclasses.MISSING}
            test.assertEqual({node.target.id for node in fields if node.value is not None}, optional)
        methods = {node.name: node for node in stub.body if isinstance(node, ast.FunctionDef)}
        public = {method for method, obj in vars(cls).items()
            if not method.startswith('_') and (isinstance(obj, (property, classmethod)) or inspect.isfunction(obj))}
        test.assertTrue(public <= methods.keys(), (key, public-methods.keys()))
        for method, node in methods.items():
            obj = inspect.getattr_static(cls, method)
            if isinstance(obj, property):
                test.assertTrue(any(isinstance(d, ast.Name) and d.id == 'property' for d in node.decorator_list))
                obj = obj.fget
            elif isinstance(obj, classmethod):
                test.assertTrue(any(isinstance(d, ast.Name) and d.id == 'classmethod' for d in node.decorator_list))
                obj = obj.__func__
            signature(obj, node)
    functions = {node.name: node for node in tree.body if isinstance(node, ast.FunctionDef) and not node.name.startswith('_')}
    public = {key: value for key, value in vars(module).items()
        if inspect.isfunction(value) and value.__module__ == name and not key.startswith('_')}
    test.assertEqual(set(functions), set(public))
    for key, obj in public.items():
        signature(obj, functions[key])
