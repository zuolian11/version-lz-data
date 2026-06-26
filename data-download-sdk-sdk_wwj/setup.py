from setuptools import setup, find_packages

setup(
    name="loongdata",
    version="0.1.3",
    packages=find_packages(),
    python_requires='>=3.6',
    install_requires=[
        "tqdm",
        "requests>=2.20.0",
        "argparse>=1.1",
        "httpx>=0.28.1",
        "rich>=14.2.0",
        "esdk-obs-python>=3.25.3"
    ],
    entry_points={
        'console_scripts': [
            'loongdata=loongdata.cli:main',
        ],
    },
    # 其他元数据...
)
