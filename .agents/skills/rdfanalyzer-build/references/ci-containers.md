# CI workflows and self-hosted runner containers

Use for repository CI changes, reproducing a failed workflow, or maintaining its runner image. Read the selected YAML and runner scripts; triggering CI or replacing a runner is an execution action separate from a source review.

## Match the workflow to its actual checks

[ci.yml](../../../../.github/workflows/ci.yml) checks out recursive submodules on a self-hosted Linux runner, verifies ROOT, installs [requirements-ci.txt](../../../../requirements-ci.txt), configures with CUDA paths disabled, builds and runs CTest. Compare runner environment, configured cache and registered tests with the failing local job. It does not currently build the runner image as part of each job.

[analysis-doublemu.yml](../../../../.github/workflows/analysis-doublemu.yml) is manually dispatched. It builds/runs the optional CMS DoubleMuon analysis and constructs analytic Z-peak fit artifacts with a ROOT-compatible interpreter. Trace injected thread config, cwd, actual meta/skim outputs, environment exports into embedded Python, and uploaded artifact paths. Its native dependency options differ from the basic CI workflow. Workflow comments and green shell steps are insufficient proof that the expected histograms, workspace or fit were checked.

[jekyll-gh-pages.yml](../../../../.github/workflows/jekyll-gh-pages.yml) builds and deploys documentation. Use [documentation maintenance](../../rdfanalyzer-development/references/documentation.md) for the Doxygen/search/Jekyll sequence. Changes to the main branch can trigger deployment; source edits do not require manually dispatching it.

## Follow the runner lifecycle

[docker/gh-runner.Dockerfile](../../../../docker/gh-runner.Dockerfile) builds the runner and ROOT environment, installs compiler/Python dependencies, and checks PyROOT during image build. Verify the current pinned downloads, architecture and Python ABI from the file. Rebuilding this image is not implied by editing analysis code.

[docker/start.sh](../../../../docker/start.sh) obtains a registration token, removes stale local runner state, registers an ephemeral runner and starts it. [startupContainer.sh](../../../../docker/startupContainer.sh) launches named containers; its `--force` option removes existing containers first. Inspect existing containers and requested scope before using that option. Runner credentials come from the configured environment; inspect presence/names without printing secrets.

Validate changed YAML and shell logic with the appropriate local parsers/static checks, then reproduce the affected command with matched dependencies when available. Do not count a source review as a successful CI run or a container launch as completed tests. Capture build/test logs and registered test coverage separately from runner/deployment status.
