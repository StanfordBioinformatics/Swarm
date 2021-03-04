FROM python:3

RUN apt-get update \
  && apt-get -y --allow-unauthenticated install software-properties-common \
  && add-apt-repository "deb https://cloud.r-project.org/bin/linux/ubuntu $(lsb_release -cs)-cran35/" \
  && apt-key adv --keyserver keyserver.ubuntu.com --recv-keys E298A3A825C0D65DFD57CBB651716619E084DAB9 \
  && apt-get -y --allow-unauthenticated install r-base \
  && apt-get install -y -qq curl zip python3-pip \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src

COPY requirements.txt /usr/src/

RUN pip3 install -r requirements.txt

COPY . /usr/src/

RUN chmod +x *.sh \
  && ln -s /usr/src/plink/plink /usr/local/bin/plink \
  && ln -s /usr/src/plink/prettify /usr/local/bin/prettify

ENTRYPOINT [ "/usr/src/entrypoint.sh" ]
