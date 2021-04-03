# Dependencies: singularity 3.7+, Make, sudo, tested under debian 11 (bullseye) and debian 10 (buster).
all: pythonenv

environment.sif: py3_9_scipy_graphviz.def
	sudo singularity build environment.sif py3_9_scipy_graphviz.def

pythonenv: environment.sif
	singularity exec environment.sif python3 -m venv code 
	singularity exec environment.sif source code/bin/activate
	singularity exec environment.sif pip3 install -i requirements.txt

.PHONY: pythonenv
