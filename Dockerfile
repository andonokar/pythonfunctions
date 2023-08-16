# Define custom function directory
ARG FUNCTION_DIR="/function"

FROM python:3.11 as build-image

# Include global arg in this stage of the build
ARG FUNCTION_DIR

# Copy function code
RUN mkdir -p ${FUNCTION_DIR}
COPY . ${FUNCTION_DIR}

RUN apt-get update && apt-get install -y build-essential

# Install the specified packages
RUN pip install -r ${FUNCTION_DIR}/requirements.txt
WORKDIR ${FUNCTION_DIR}/compiled
RUN python3 setup.py build_ext --inplace
RUN rm setup.py
RUN rm extractclass.pyx
RUN rm extractclass.c
RUN rm -rf build

FROM public.ecr.aws/lambda/python:3.11

ARG FUNCTION_DIR
# Copy in the built dependencies
COPY --from=build-image ${FUNCTION_DIR}/. ${LAMBDA_TASK_ROOT}
RUN pip install -r requirements.txt

# Pass the name of the function handler as an argument to the runtime
CMD ["app.handler"]
