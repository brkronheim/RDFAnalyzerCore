"""Independent Python-authored physics for the proposed onboarding comparison."""
import numpy as np


def weight(value):
    return np.float64(value)


def observable(value):
    return np.float64(value)*2.


def selected(value):
    return value != 0.
