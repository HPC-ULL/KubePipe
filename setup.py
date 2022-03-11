from distutils.core import setup
setup(
  name = 'KubePipe',        
  packages = ['KubePipe'],  
  version = '0.1.2',      
  license='MIT',       
  description = 'Tool to paralelize execution of multiple pipelines of machine learning using kubernetes',   
  author = 'Daniel Suárez Labena',                 
  author_email = 'alu0101040882@ull.edu,es',    
  url = 'https://github.com/alu0101040882/kubernetes-ml-pipeline-TFM',  
  download_url = 'https://github.com/alu0101040882/kubernetes-ml-pipeline-TFM',   
  keywords = ['Kubernetes','Machine learning'],   
  install_requires=[           
        'argo-workflows==6.3.0rc2',
        'kubernetes',
        'minio',
        'PyYAML',
        'scikit_learn==1.0.2'
      ],
  classifiers=[
    'Development Status :: 3 - Alpha',      
    'Intended Audience :: Developers',      
    'Topic :: Software Development :: Build Tools',
    'License :: OSI Approved :: MIT License',   
    'Programming Language :: Python :: 3',      
    'Programming Language :: Python :: 3.4',
    'Programming Language :: Python :: 3.5',
    'Programming Language :: Python :: 3.6',
  ],
)