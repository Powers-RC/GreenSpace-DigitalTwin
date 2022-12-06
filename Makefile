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
		
.PHONY: bundle-aurora-lambda
bundle-aurora-lambda:
	@(cd enviro_digital_twin/lambdas/aurora_processing_lambda && \
		rm -rf .build && \
		mkdir -p .build/dependencies && \
		poetry export --without-hashes -f requirements.txt -o .build/requirements.txt && \
		pip install --no-cache -r .build/requirements.txt -t .build/dependencies/ && \
		cd .build/dependencies && \
		zip -r ../aurora-lambda-deployment.zip . && \
		cd .. && \
		zip aurora-lambda-deployment.zip ../aurora_processing_lambda.py)
