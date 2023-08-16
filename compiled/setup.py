from setuptools import setup
from Cython.Build import cythonize

setup(
    name='test-app',
    ext_modules=cythonize("extractclass.pyx"),
)
