# Dependencies: singularity 3.7+, Make, sudo, tested under debian 11 (bullseye) and debian 10 (buster).
all: pythonenv

environment.sif: py3_9_scipy_graphviz.def
	sudo singularity build environment.sif py3_9_scipy_graphviz.def

pythonenv: environment.sif setup_pyvenv.sh
	singularity exec environment.sif ./setup_pyvenv.sh

.PHONY: pythonenv
