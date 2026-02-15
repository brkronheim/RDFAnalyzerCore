#!/bin/bash
unset ROOTSYS
ulimit -n 10000
# source /cvmfs/sft.cern.ch/lcg/app/releases/ROOT/6.30.02/x86_64-almalinux9.3-gcc114-opt/bin/thisroot.sh
source /cvmfs/sft.cern.ch/lcg/views/LCG_108a/x86_64-el9-gcc15-opt/setup.sh
# Ensure any ROOTSYS set by the view's setup is cleared so CMake sees no mismatch
unset ROOTSYS