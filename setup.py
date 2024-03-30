"""
Install this Python package.
"""

import re
import os.path
from setuptools import setup, find_packages


class Setup:
    """Convenience wrapper (for C.I. purposes) of the `setup()` call form `setuptools`."""

    def __init__(self, **kw):
        self.conf = kw
        self.work_dir = os.path.abspath(os.path.dirname(__file__))

        # Automatically fill `package_data` from `MANIFEST.in`. No need to repeat lists twice
        assert "package_data" not in self.conf
        assert "include_package_data" not in self.conf
        package_data = {}
        with open(os.path.join(self.work_dir, "MANIFEST.in"), encoding="utf-8") as fp:
            for line in fp.readlines():
                line = line.strip()
                m = re.search(r"include\s+(.+)/([^/]+)", line)
                assert m
                module = m.group(1).replace("/", ".")
                filename = m.group(2)
                if module not in package_data:
                    package_data[module] = []
                package_data[module].append(filename)
        if package_data:
            self.conf["include_package_data"] = True
            self.conf["package_data"] = package_data

        # Automatically fill the long description from README. Filter out lines that look like
        # "badges".
        assert "long_description" not in self.conf
        assert "long_description_content_type" not in self.conf
        with open(os.path.join(self.work_dir, "README.md"), encoding="utf-8") as fp:
            ld = "\n".join([row for row in fp if not row.startswith("[![")])
        self.conf["long_description"] = ld
        self.conf["long_description_content_type"] = "text/markdown"

    def __str__(self):
        return str(self.conf)

    def __call__(self):
        setup(**self.conf)


SETUP = Setup(
    name="RBR_Klassement",
    version="0.0.1",
    description="Tool for classification of the Run-Bike-Run series 2024",
    url="https://github.com/lvermunt/RBR_Klassement",
    author="luuk-vermunt",
    author_email="",
    license="MIT",
    # See https://pypi.org/classifiers/
    classifiers=[],
    # What does your project relate to?
    keywords="",
    # You can just specify the packages manually here if your project is simple. Or you can use
    # find_packages().
    packages=find_packages(),
    # List run-time dependencies here. These will be installed by pip when your project is
    # installed. For an analysis of "install_requires" vs pip's requirements files see:
    # https://packaging.python.org/en/latest/requirements.html
    install_requires=[
        "pandas==2.2.1",
        "openpyxl==3.1.2",
    ],
    python_requires=">3.11.1",
    # List additional groups of dependencies here (e.g. development dependencies). You can install
    # these using the following syntax, for example:
    # $ pip install -e .[dev]
    # for zsh: $ pip install -e .\[dev\]
    extras_require={
        "dev": ["pylint>=3.0.3", "flake8>=6.1.0", "pytest>=6.2.2", "setuptools>=7.4.3"]
    },
    # Although 'package_data' is the preferred approach, in some case you may need to place data
    # files outside of your packages. See:
    # http://docs.python.org/3.4/distutils/setupscript.html#installing-additional-files
    data_files=[],
    # To provide executable scripts, use entry points in preference to the "scripts" keyword. Entry
    # points provide cross-platform support and allow pip to create the appropriate form of
    # executable for the target platform. See:
    # https://chriswarrick.com/blog/2014/09/15/python-apps-the-right-way-entry_points-and-scripts/
    entry_points={
        "console_scripts": ["RBR_Klassement = classification:entrypoint"]
    },
)

if __name__ == "__main__":
    SETUP()
