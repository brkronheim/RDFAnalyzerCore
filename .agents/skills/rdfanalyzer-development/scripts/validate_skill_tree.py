#!/usr/bin/env python3
"""Validate repository skills, routing links, plugin coverage, and LAW task routes.

Requires Python 3.9+ and PyYAML. Defaults resolve from this file; --repo/--skills support a staged
skill tree or another checkout. Optional analysis links are checked when present.
"""

import argparse
import ast
from collections import deque
from pathlib import Path
import re
import sys
from urllib.parse import unquote, urlsplit

import yaml


LINK = re.compile(r"\[[^\]\n]*\]\(([^)\n]+)\)")
NAME = re.compile(r"[a-z0-9]+(?:-[a-z0-9]+)*\Z")


def within(path, parent):
    try:
        path.relative_to(parent)
        return True
    except ValueError:
        return False


def read_yaml(path, errors):
    try:
        return yaml.safe_load(path.read_text())
    except (OSError, yaml.YAMLError) as exc:
        errors.append(f"{path}: {exc}")
        return None



def discover_law_tasks(repo, errors):
    """Find concrete task classes through local inheritance without importing LAW."""
    tasks = set()
    modules = list((repo/'core/python/law').glob('*_tasks.py'))
    modules += list((repo/'extensions/cms/python/rdfanalyzer_cms').glob('*_tasks.py'))
    for module in sorted(modules):
        try:
            tree = ast.parse(module.read_text(), filename=str(module))
        except (OSError, SyntaxError) as exc:
            errors.append(f'{module}: cannot inspect LAW task classes: {exc}')
            continue
        classes = [node for node in tree.body if isinstance(node, ast.ClassDef)]
        selected = set()
        changed = True
        while changed:
            changed = False
            for node in classes:
                bases = {ast.unparse(base) for base in node.bases}
                if node.name not in selected and bases & ({'law.Task', 'law.LocalWorkflow', 'PlotTask'} | selected):
                    selected.add(node.name)
                    changed = True
        tasks.update((str(module.relative_to(repo)), name) for name in selected)
    return tasks


def validate_reference(value, skill, skills, errors, context):
    if not isinstance(value, str) or Path(value).is_absolute():
        errors.append(f'{context}: reference must be skill-root relative')
        return None
    path = (skills/value).resolve()
    if not within(path, skills) or not path.is_file() or path.suffix != '.md':
        errors.append(f'{context}: missing or invalid instruction reference: {value}')
        return None
    if path.relative_to(skills).parts[0] != skill:
        errors.append(f'{context}: reference does not belong to routed skill: {value}')
    return path


def validate_law_routes(inventory, repo, skills, names, component_paths, errors):
    actual = discover_law_tasks(repo, errors)
    routes = inventory.get('law_tasks', []) if isinstance(inventory, dict) else []
    if not isinstance(routes, list):
        errors.append('LAW task routes must be a list')
        routes = []
    documented = set()
    for entry in routes:
        if not isinstance(entry, dict):
            errors.append('LAW task route must be a mapping')
            continue
        module, name = entry.get('module'), entry.get('class')
        if not isinstance(module, str) or not isinstance(name, str):
            errors.append('LAW task route requires module and class strings')
            continue
        key = (module, name)
        context = f'LAW task {module}:{name}'
        if key in documented:
            errors.append(f'{context}: duplicate task route')
        if not (entry.get("optional") and not (repo/module).exists()):
            documented.add(key)
        if entry.get('skill') not in names:
            errors.append(f'{context}: unknown skill')
        ref = validate_reference(entry.get('reference'), entry.get('skill'), skills, errors, context)
        if ref and not re.search(r'\b' + re.escape(name) + r'\b', ref.read_text()):
            errors.append(f'{context}: class not named in routed reference')
    for module, name in sorted(actual - documented):
        errors.append(f'LAW task has no skill route: {module}:{name}')
    for module, name in sorted(documented - actual):
        errors.append(f'LAW task route has no concrete class: {module}:{name}')
    law_root = repo/'core/python/law'
    runtime_files = {
        str(path.relative_to(repo)) for path in law_root.iterdir()
        if path.is_file() and not path.name.startswith('test_')
        and (path.suffix in {'.py', '.sh'} or path.name == 'law.cfg')
    } if law_root.is_dir() else set()
    for module in sorted(runtime_files - component_paths):
        errors.append(f'LAW runtime module has no explicit skill route: {module}')
    return len(actual), len(runtime_files)


def validate(repo, skills):
    errors, notes = [], set()
    extension_skills = sorted((repo/'extensions').glob('*/.agents/skills'))
    skill_roots = [skills, *extension_skills]
    entries = sorted(p for tree in skill_roots for p in tree.glob('*/SKILL.md'))
    if not entries:
        return ['No skills found'], notes, {}
    names = {p.parent.name for p in entries}
    for entry in entries:
        raw = entry.read_text()
        if not raw.startswith('---\n') or '\n---\n' not in raw[4:]:
            errors.append(f'{entry}: missing YAML frontmatter')
            continue
        front, body = raw[4:].split('\n---\n', 1)
        try:
            meta = yaml.safe_load(front)
        except yaml.YAMLError as exc:
            errors.append(f'{entry}: {exc}')
            continue
        if not isinstance(meta, dict):
            errors.append(f'{entry}: frontmatter must be a mapping')
            continue
        name = meta.get('name', '')
        if not isinstance(name, str) or not NAME.fullmatch(name) or len(name) >= 64 or name != entry.parent.name:
            errors.append(f'{entry}: invalid or mismatched skill name')
        if not isinstance(meta.get('description'), str) or not meta['description'].strip():
            errors.append(f'{entry}: missing description')
        if not body.strip() or '[TODO' in body or '[TBD' in body:
            errors.append(f'{entry}: empty or unfinished instructions')
        ui_path = entry.parent/'agents/openai.yaml'
        ui = read_yaml(ui_path, errors)
        interface = ui.get('interface', {}) if isinstance(ui, dict) else {}
        if not isinstance(interface, dict):
            interface = {}
        for field in ['display_name', 'short_description', 'default_prompt']:
            if not isinstance(interface.get(field), str) or not interface[field].strip():
                errors.append(f'{ui_path}: missing interface.{field}')
        short = interface.get('short_description', '')
        if not isinstance(short, str) or not 25 <= len(short) <= 64:
            errors.append(f'{ui_path}: short_description must be 25-64 characters')
        if f'${name}' not in str(interface.get('default_prompt', '')):
            errors.append(f'{ui_path}: default_prompt must name ${name}')

    inventory_path = skills/'rdfanalyzer-development/references/components.yaml'
    inventory = read_yaml(inventory_path, errors)
    components = inventory.get('components', []) if isinstance(inventory, dict) else []
    if not isinstance(components, list) or not components:
        errors.append(f'{inventory_path}: missing components list')
        components = []
    if not isinstance(inventory, dict) or inventory.get('version') != 1:
        errors.append(f'{inventory_path}: unsupported inventory version')
    documented_plugins, optional_roots, seen_paths = set(), [], set()
    for component in components:
        if not isinstance(component, dict):
            errors.append(f'{inventory_path}: component must be a mapping')
            continue
        raw_path = component.get('path')
        if not isinstance(raw_path, str) or Path(raw_path).is_absolute():
            errors.append(f'{inventory_path}: component path must be repository relative')
            continue
        path = (repo/raw_path).resolve()
        if not within(path, repo):
            errors.append(f'{inventory_path}: component escapes repository: {raw_path}')
            continue
        if raw_path in seen_paths:
            errors.append(f'{inventory_path}: duplicate component: {raw_path}')
        seen_paths.add(raw_path)
        if component.get('skill') not in names:
            errors.append(f'{inventory_path}: unknown skill for {raw_path}')
        if 'reference' in component:
            validate_reference(component['reference'], component.get('skill'), skills, errors, raw_path)
        if component.get('optional'):
            optional_roots.append(path)
        if not path.exists():
            if component.get('optional'):
                notes.add(f'Optional checkout absent: {raw_path}')
            else:
                errors.append(f'{inventory_path}: missing component: {raw_path}')
        if component.get('kind') == 'plugin' and (path.exists() or not component.get('optional')):
            documented_plugins.add(raw_path)

    law_count, law_modules = validate_law_routes(
        inventory, repo, skills, names, seen_paths, errors
    )

    actual_plugins = set()
    for plugin_root in (repo/'core/plugins', repo/'extensions/cms/plugins'):
        if not plugin_root.is_dir():
            continue
        actual_plugins.update(
            str(p.relative_to(repo)) for p in plugin_root.iterdir()
            if p.is_dir() and (p/'CMakeLists.txt').exists()
        )
        actual_plugins.update(
            str(p.parent.relative_to(repo)) for p in plugin_root.rglob('*Manager.h')
            if 'IPluggableManager' in p.read_text()
        )
        if (plugin_root/'ObjectEnergyManagerBase').is_dir():
            actual_plugins.add(str((plugin_root/'ObjectEnergyManagerBase').relative_to(repo)))
    for path in sorted(actual_plugins - documented_plugins):
        errors.append(f'Plugin has no skill route: {path}')
    for path in sorted(documented_plugins - actual_plugins):
        errors.append(f'Inventory plugin no longer found: {path}')

    # Resolve repo-relative source links from a staged tree as if installed.
    installed_root = repo/'.agents/skills'
    graph = {}
    markdown = sorted(p for tree in skill_roots for p in tree.rglob('*.md'))
    for source in markdown:
        source = source.resolve()
        targets = set()
        for match in LINK.finditer(source.read_text()):
            target = match.group(1).strip().strip('<>')
            if not target or target.startswith('#'):
                continue
            url = urlsplit(target)
            if url.scheme or url.netloc:
                continue
            virtual_source = (installed_root/source.relative_to(skills)
                              if within(source, skills) else source)
            virtual_target = (virtual_source.parent/unquote(url.path)).resolve()
            if within(virtual_target, installed_root):
                resolved = (skills/virtual_target.relative_to(installed_root)).resolve()
            else:
                resolved = virtual_target
            if not resolved.exists():
                missing_optional = any(within(virtual_target, root) and not root.exists() for root in optional_roots)
                if not missing_optional:
                    errors.append(f'{source}: broken link: {target}')
            elif resolved.suffix == '.md' and any(within(resolved, tree) for tree in skill_roots):
                targets.add(resolved)
        graph[source] = targets

    roots = [(skills/f'{name}/SKILL.md').resolve() for name in ['rdfanalyzer-analysis', 'rdfanalyzer-development']]
    pending, reachable = deque(roots), set()
    while pending:
        current = pending.popleft()
        if current in reachable:
            continue
        reachable.add(current)
        pending.extend(graph.get(current, ()))
    for path in markdown:
        if path.resolve() not in reachable:
            errors.append(f'Unreachable instructions: {path}')

    counts = {'skills': len(entries), 'references': sum(1 for p in markdown if p.name != 'SKILL.md'),
              'plugins': len(actual_plugins), 'components': len(components),
              'LAW tasks': law_count, 'LAW runtime files': law_modules}
    return errors, notes, counts


def main():
    default_skills = Path(__file__).resolve().parents[2]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo', type=Path, default=default_skills.parents[1])
    parser.add_argument('--skills', type=Path, default=default_skills)
    args = parser.parse_args()
    errors, notes, counts = validate(args.repo.resolve(), args.skills.resolve())
    for note in sorted(notes):
        print(f'NOTE: {note}')
    for error in errors:
        print(f'ERROR: {error}', file=sys.stderr)
    if errors:
        print(f'Validation failed: {len(errors)} error(s)', file=sys.stderr)
        return 1
    print('Validated ' + ', '.join(f'{value} {key}' for key, value in counts.items()))
    return 0


if __name__ == '__main__':
    sys.exit(main())
