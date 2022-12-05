.PHONY: bundle-data-lambda
bundle-data-lambda:
	@(cd enviro_digital_twin/lambdas/data_processing_lambda && \
		rm -rf build && \
		mkdir -p build && \
		poetry export --without-hashes -f requirements.txt -o requirements.txt && \
		pip install --no-cache -r requirements.txt -t build/ && \
		cd build && \
		zip -r ../data-lambda-deployment.zip . && \
		cd .. && \
		zip data-lambda-deployment.zip data_processing_lambda.py)
