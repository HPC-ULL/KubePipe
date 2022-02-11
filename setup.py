from distutils.core import setup
setup(
  name = 'KubePipe',         # How you named your package folder (MyLib)
  packages = ['KubePipe'],   # Chose the same as "name"
  version = '0.1',      # Start with a small number and increase it with every change you make
  license='MIT',        # Chose a license from here: https://help.github.com/articles/licensing-a-repository
  description = 'Tool to paralelize execution of multiple pipelines of machine learning using kubernetes',   # Give a short description about your library
  author = 'Daniel Suárez Labena',                   # Type in your name
  author_email = 'alu0101040882@ull.edu,es',      # Type in your E-Mail
  url = 'https://github.com/alu0101040882/kubernetes-ml-pipeline-TFM',   # Provide either the link to your github or to your website
  download_url = 'https://github.com/alu0101040882/kubernetes-ml-pipeline-TFM',    # I explain this later on
  keywords = ['Kubernetes','Machine learning'],   # Keywords that define your package best
  install_requires=[            # I get to this in a second
        'argo-workflows==6.3.0rc2',
        'kubernetes',
        'minio',
        'PyYAML',
        'scikit_learn==1.0.2'
      ],
  classifiers=[
    'Development Status :: 3 - Alpha',      # Chose either "3 - Alpha", "4 - Beta" or "5 - Production/Stable" as the current state of your package
    'Intended Audience :: Developers',      # Define that your audience are developers
    'Topic :: Software Development :: Build Tools',
    'License :: OSI Approved :: MIT License',   # Again, pick a license
    'Programming Language :: Python :: 3',      #Specify which pyhton versions that you want to support
    'Programming Language :: Python :: 3.4',
    'Programming Language :: Python :: 3.5',
    'Programming Language :: Python :: 3.6',
  ],
)