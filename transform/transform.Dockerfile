FROM public.ecr.aws/lambda/python:3.8

COPY cleaning_requirements.txt .
RUN pip3 install -r cleaning_requirements.txt --target "${LAMBDA_TASK_ROOT}"

COPY cleaning.py .env ${LAMBDA_TASK_ROOT}

CMD [ "transform.handler" ]