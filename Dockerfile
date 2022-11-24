#Cuda k40m energy

FROM  nvidia/cuda:10.2-base-ubuntu18.04

# Install some basic utilities
RUN apt-get update && apt-get install -y \
    curl \
    ca-certificates \
    sudo \
    git \
    bzip2 \
    libx11-6 \
 && rm -rf /var/lib/apt/lists/*

# Create a working directory
RUN mkdir /app
WORKDIR /app

# # Create a non-root user and switch to it
# RUN adduser --disabled-password --gecos '' --shell /bin/bash user \
#  && chown -R user:user /app
# RUN echo "user ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/90-user
# USER user

# # All users can use /home/user as their home directory
# ENV HOME=/home/user
# RUN chmod 777 /home/user

# Install Miniconda and Python 3.8
ENV CONDA_AUTO_UPDATE_CONDA=false
ENV PATH=/root/miniconda/bin:$PATH
RUN curl -sLo ~/miniconda.sh https://repo.anaconda.com/miniconda/Miniconda3-py39_4.12.0-Linux-x86_64.sh \
 && chmod +x ~/miniconda.sh \
 && ~/miniconda.sh -b -p ~/miniconda \
 && rm ~/miniconda.sh \
 && conda install -y python==3.9.13 \
 && conda clean -ya


# CUDA 10.2-specific steps
RUN conda install -y -c nvidia cudatoolkit \
 && conda clean -ya
 
RUN pip install  torch==1.10.0+cu102 -f https://nelsonliu.me/files/pytorch/whl/torch_stable.html torchvision
RUN conda install dill 
RUN conda install scikit-learn
RUN conda install minio

# conda install dill scikit-learn minio matplotlib pandas

# pip install -e KubePipe

RUN apt update -y && apt-get install build-essential pkg-config libconfuse-dev -y

RUN curl -L https://github.com/HPC-ULL/eml/releases/download/v1.0.1/eml-1.0.1.tar.gz --output  eml.tar.gz && tar -xvf eml.tar.gz && rm eml.tar.gz

RUN conda install libgcc

RUN cd eml-1.0.1 \
    && ./configure  \ 
    && make \
    && make install

RUN mv /usr/local/lib/libeml.so.0.0.0 /usr/local/lib/libeml.so.1.1

COPY . KubePipe/

RUN cd KubePipe && echo -n "" > kube_pipe/__init__.py && pip3 install -e . --no-deps

RUN cp KubePipe/test/energy/PyEML/pyeml.cpython-39-x86_64-linux-gnu.so . && cp KubePipe/test/performance/pyemlWrapper.py pyemlWrapper.py

ENV LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH:/root/miniconda/lib

RUN echo "alias python='python3'" > /root/.bashrc

CMD ["python"]

#sudo docker run --gpus all --rm -it -v /dev/cpu:/dev/cpu  --privileged --network=host alu0101040882/kubepipe:3.9.2-cuda-k40m-energy  bash