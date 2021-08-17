import codecs
import os.path

from setuptools import setup

author_dict = {
    "Luca Carniato": "luca.carniato@gmail.com",
}
__author__ = ", ".join(author_dict.keys())
__author_email__ = ", ".join(s for _, s in author_dict.items())


def read(rel_path: str) -> str:
    """Used to read a text file

    Args:
        rel_path (str): Relative path to the file

    Returns:
        str: File content
    """
    here = os.path.abspath(os.path.dirname(__file__))
    with codecs.open(os.path.join(here, rel_path), "r") as fp:
        return fp.read()


try:
    from wheel.bdist_wheel import bdist_wheel as _bdist_wheel

    class bdist_wheel(_bdist_wheel):
        """Class describing our wheel.
        Basically it says that it is not a pure Python package,
        but it also does not contain any Python source and
        therefore works for all Python versions
        """

        def finalize_options(self):
            _bdist_wheel.finalize_options(self)
            # Mark us as a pure python package
            self.root_is_pure = True

        def get_tag(self):
            python, abi, plat = _bdist_wheel.get_tag(self)
            # We don't contain any python source
            python, abi = "py3", "none"
            return python, abi, plat


except ImportError:
    bdist_wheel = None


long_description = read("README.md")

setup(
    name="weathercalculator",
    description="`weathercalculator` calculates head waves and cold waves.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author=__author__,
    author_email=__author_email__,
    platforms="Windows, Linux",
    install_requires=["pyspark"],
    extras_require={
        "tests": ["pytest", "pytest-cov"],
        "lint": [
            "black==21.4b1",
            "isort",
        ],
    },
    python_requires=">=3.8",
    packages=["weathercalculator"],
    cmdclass={"bdist_wheel": bdist_wheel},
)
