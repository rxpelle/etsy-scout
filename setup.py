from setuptools import setup, find_packages

setup(
    name='etsy-scout',
    version='0.1.0',
    description='Etsy keyword research and competitor analysis tool',
    author='Randy Pellegrini',
    url='https://github.com/rxpelle/etsy-scout',
    license='MIT',
    packages=find_packages(),
    python_requires='>=3.9',
    install_requires=[
        'requests',
        'beautifulsoup4',
        'click',
        'rich',
        'python-dotenv',
        'pandas',
    ],
    extras_require={
        'dev': [
            'pytest',
            'pytest-cov',
        ],
    },
    entry_points={
        'console_scripts': [
            'etsy-scout=etsy_scout.cli:main',
        ],
    },
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: End Users/Desktop',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
        'Topic :: Office/Business',
    ],
)
