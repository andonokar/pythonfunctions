from setuptools import setup
from Cython.Build import cythonize

setup(
    name='test-app',
    ext_modules=cythonize([f"*.py", '*/*.py'],
                          ['__init__.py', '*/__init__.py', 'setup.py', '*/setup.py', 'app.py', 'main.py', 'test.py'])
)
