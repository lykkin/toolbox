FROM numenta/nupic
ENV PYTHONUNBUFFERED 1
COPY ./anomaly-detector/requirements.txt /
RUN pip install -r /requirements.txt

ADD ./anomaly-detector/src /src
WORKDIR /src
CMD ["python", "./main.py"]
