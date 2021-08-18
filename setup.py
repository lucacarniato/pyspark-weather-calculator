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
        "tests": ["pytest"],
        "weathercalculator": ["tabulate"],
        "lint": [
            "black==21.4b1",
            "isort",
        ],
    },
    python_requires=">=3.8",
    packages=["weathercalculator"],
)
