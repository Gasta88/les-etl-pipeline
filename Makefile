PROJECT_ID ?= dataops-369610
REGION ?= europe-west3
PROJECT_NUMBER ?= $$(gcloud projects list --filter=${PROJECT_ID} --format="value(PROJECT_NUMBER)")
CODE_BUCKET ?= data-lake-code-${PROJECT_NUMBER}
TEMP_BUCKET ?= data-lake-staging-${PROJECT_NUMBER}
DATA_BUCKET ?= fgasta_test
APP_NAME ?= $$(cat pyproject.toml| grep name | cut -d" " -f3 | sed  's/"//g')
VERSION_NO ?= $$(poetry version --short)
SRC_WITH_DEPS ?= src_with_deps
DELTA_JAR_FILE ?= delta-core_2.13-2.1.0.jar

.PHONY: $(shell sed -n -e '/^$$/ { n ; /^[^ .\#][^ ]*:/ { s/:.*$$// ; p ; } ; }' $(MAKEFILE_LIST))

.DEFAULT_GOAL := help

help: ## This is help
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

setup: ## Setup Buckets and Dataset for Demo
	@echo "Project=${PROJECT_ID}--${PROJECT_NUMBER}--${CODE_BUCKET}--${TEMP_BUCKET}"
	@gsutil mb -c standard -l ${REGION} -p ${PROJECT_ID} gs://${CODE_BUCKET}
	@gsutil mb -c standard -l ${REGION} -p ${PROJECT_ID} gs://${TEMP_BUCKET}
	@echo "The Following Buckets created - ${CODE_BUCKET}, ${TEMP_BUCKET}, ${DATA_BUCKET}"
	@echo "Create Hive Metastore"
	# @gcloud metastore services create data-catalog-${PROJECT_ID} --hive-metastore-version=3.1.2 --location=${REGION}

clean: ## CleanUp Prior to Build
	@rm -Rf ./dist
	@rm -Rf ./${SRC_WITH_DEPS}
	@rm -f requirements.txt

build: clean ## Build Python Package with Dependencies
	@echo "Packaging Code and Dependencies for ${APP_NAME}-${VERSION_NO}"
	@mkdir -p ./dist
	@poetry update
	@poetry export -f requirements.txt --without-hashes -o requirements.txt
	@poetry run pip install . -r requirements.txt -t ${SRC_WITH_DEPS}
	@cd ./${SRC_WITH_DEPS}
	@find . -name "*.pyc" -delete
	@cd ./${SRC_WITH_DEPS} && zip -x "*.git*" -x "*.DS_Store" -x "*.pyc" -x "*/*__pycache__*/" -x ".idea*" -r ../dist/${SRC_WITH_DEPS}.zip .
	@rm -Rf ./${SRC_WITH_DEPS}
	@rm -f requirements.txt
	@cp ./src/main.py ./dist
	@mv ./dist/${SRC_WITH_DEPS}.zip ./dist/${APP_NAME}_${VERSION_NO}.zip
	@gsutil cp -r ./dist gs://${CODE_BUCKET}
	@gsutil cp -r dependencies/*.jar gs://${CODE_BUCKET}/dependencies/


run_asset_bronze: ## Run the dataproc serverless job
	# @gcloud compute networks subnets update default \
	# --region=${REGION} \
	# --enable-private-ip-google-access
	@gcloud dataproc batches submit --project ${PROJECT_ID} --region ${REGION} pyspark \
	gs://${CODE_BUCKET}/dist/main.py --py-files=gs://${CODE_BUCKET}/dist/${APP_NAME}_${VERSION_NO}.zip \
	--subnet default --properties spark.executor.instances=4,spark.driver.cores=8,spark.executor.cores=8,spark.app.name=loan_etl_pipeline \
	--jars gs://${CODE_BUCKET}/dependencies/${DELTA_JAR_FILE},gs://${CODE_BUCKET}/dependencies/delta-storage-2.2.0.jar \
	--metastore-service=projects/${PROJECT_ID}/locations/${REGION}/services/data-catalog-${PROJECT_ID} \
	--version=2.0 \
	-- --project=${PROJECT_ID} --bucket-name=${DATA_BUCKET} --source-prefix=mini_source --target-prefix=SME/bronze/assets --file-key=Loan_Data --stage-name=bronze_asset

run_collateral_bronze: ## Run the dataproc serverless job
	# @gcloud compute networks subnets update default \
	# --region=${REGION} \
	# --enable-private-ip-google-access
	@gcloud dataproc batches submit --project ${PROJECT_ID} --region ${REGION} pyspark \
	gs://${CODE_BUCKET}/dist/main.py --py-files=gs://${CODE_BUCKET}/dist/${APP_NAME}_${VERSION_NO}.zip \
	--subnet default --properties spark.executor.instances=4,spark.driver.cores=8,spark.executor.cores=8,spark.app.name=loan_etl_pipeline \
	--jars gs://${CODE_BUCKET}/dependencies/${DELTA_JAR_FILE},gs://${CODE_BUCKET}/dependencies/delta-storage-2.2.0.jar \
	--metastore-service=projects/${PROJECT_ID}/locations/${REGION}/services/data-catalog-${PROJECT_ID} \
	--version=2.0 \
	-- --project=${PROJECT_ID} --bucket-name=${DATA_BUCKET} --source-prefix=mini_source --target-prefix=SME/bronze/collaterals --file-key=Collateral --stage-name=bronze_collateral

run_bond_info_bronze: ## Run the dataproc serverless job
	# @gcloud compute networks subnets update default \
	# --region=${REGION} \
	# --enable-private-ip-google-access
	@gcloud dataproc batches submit --project ${PROJECT_ID} --region ${REGION} pyspark \
	gs://${CODE_BUCKET}/dist/main.py --py-files=gs://${CODE_BUCKET}/dist/${APP_NAME}_${VERSION_NO}.zip \
	--subnet default --properties spark.executor.instances=4,spark.driver.cores=8,spark.executor.cores=8,spark.app.name=loan_etl_pipeline \
	--jars gs://${CODE_BUCKET}/dependencies/${DELTA_JAR_FILE},gs://${CODE_BUCKET}/dependencies/delta-storage-2.2.0.jar \
	--metastore-service=projects/${PROJECT_ID}/locations/${REGION}/services/data-catalog-${PROJECT_ID} \
	--version=2.0 \
	-- --project=${PROJECT_ID} --bucket-name=${DATA_BUCKET} --source-prefix=mini_source --target-prefix=SME/bronze/bond_info --file-key=Bond_Info --stage-name=bronze_bond_info

run_amortisation_bronze: ## Run the dataproc serverless job
	# @gcloud compute networks subnets update default \
	# --region=${REGION} \
	# --enable-private-ip-google-access
	@gcloud dataproc batches submit --project ${PROJECT_ID} --region ${REGION} pyspark \
	gs://${CODE_BUCKET}/dist/main.py --py-files=gs://${CODE_BUCKET}/dist/${APP_NAME}_${VERSION_NO}.zip \
	--subnet default --properties spark.executor.instances=4,spark.driver.cores=8,spark.executor.cores=8,spark.app.name=loan_etl_pipeline \
	--jars gs://${CODE_BUCKET}/dependencies/${DELTA_JAR_FILE},gs://${CODE_BUCKET}/dependencies/delta-storage-2.2.0.jar \
	--metastore-service=projects/${PROJECT_ID}/locations/${REGION}/services/data-catalog-${PROJECT_ID} \
	--version=2.0 \
	-- --project=${PROJECT_ID} --bucket-name=${DATA_BUCKET} --source-prefix=mini_source --target-prefix=SME/bronze/amortisation --file-key=Amortization --stage-name=bronze_amortisation

run_asset_silver: ## Run the dataproc serverless job
	# @gcloud compute networks subnets update default \
	# --region=${REGION} \
	# --enable-private-ip-google-access
	@gcloud dataproc batches submit --project ${PROJECT_ID} --region ${REGION} pyspark \
	gs://${CODE_BUCKET}/dist/main.py --py-files=gs://${CODE_BUCKET}/dist/${APP_NAME}_${VERSION_NO}.zip \
	--subnet default --properties spark.executor.instances=4,spark.driver.cores=8,spark.executor.cores=8,spark.app.name=loan_etl_pipeline \
	--jars gs://${CODE_BUCKET}/dependencies/${DELTA_JAR_FILE},gs://${CODE_BUCKET}/dependencies/delta-storage-2.2.0.jar \
	--metastore-service=projects/${PROJECT_ID}/locations/${REGION}/services/data-catalog-${PROJECT_ID} \
	--version=2.0 \
	-- --project=${PROJECT_ID} --bucket-name=${DATA_BUCKET} --source-prefix=SME/bronze/assets --target-prefix=SME/silver/assets --pcds="" --stage-name=silver_asset

run_collateral_silver: ## Run the dataproc serverless job
	# @gcloud compute networks subnets update default \
	# --region=${REGION} \
	# --enable-private-ip-google-access
	@gcloud dataproc batches submit --project ${PROJECT_ID} --region ${REGION} pyspark \
	gs://${CODE_BUCKET}/dist/main.py --py-files=gs://${CODE_BUCKET}/dist/${APP_NAME}_${VERSION_NO}.zip \
	--subnet default --properties spark.executor.instances=4,spark.driver.cores=8,spark.executor.cores=8,spark.app.name=loan_etl_pipeline \
	--jars gs://${CODE_BUCKET}/dependencies/${DELTA_JAR_FILE},gs://${CODE_BUCKET}/dependencies/delta-storage-2.2.0.jar \
	--metastore-service=projects/${PROJECT_ID}/locations/${REGION}/services/data-catalog-${PROJECT_ID} \
	--version=2.0 \
	-- --project=${PROJECT_ID} --bucket-name=${DATA_BUCKET} --source-prefix=SME/bronze/collaterals --target-prefix=SME/silver/collaterals --pcds="" --stage-name=silver_collateral

run_bond_info_silver: ## Run the dataproc serverless job
	# @gcloud compute networks subnets update default \
	# --region=${REGION} \
	# --enable-private-ip-google-access
	@gcloud dataproc batches submit --project ${PROJECT_ID} --region ${REGION} pyspark \
	gs://${CODE_BUCKET}/dist/main.py --py-files=gs://${CODE_BUCKET}/dist/${APP_NAME}_${VERSION_NO}.zip \
	--subnet default --properties spark.executor.instances=4,spark.driver.cores=8,spark.executor.cores=8,spark.app.name=loan_etl_pipeline \
	--jars gs://${CODE_BUCKET}/dependencies/${DELTA_JAR_FILE},gs://${CODE_BUCKET}/dependencies/delta-storage-2.2.0.jar \
	--metastore-service=projects/${PROJECT_ID}/locations/${REGION}/services/data-catalog-${PROJECT_ID} \
	--version=2.0 \
	-- --project=${PROJECT_ID} --bucket-name=${DATA_BUCKET} --source-prefix=SME/bronze/bond_info --target-prefix=SME/silver/bond_info --pcds="" --stage-name=silver_bond_info

run_amortisation_silver: ## Run the dataproc serverless job
	# @gcloud compute networks subnets update default \
	# --region=${REGION} \
	# --enable-private-ip-google-access
	@gcloud dataproc batches submit --project ${PROJECT_ID} --region ${REGION} pyspark \
	gs://${CODE_BUCKET}/dist/main.py --py-files=gs://${CODE_BUCKET}/dist/${APP_NAME}_${VERSION_NO}.zip \
	--subnet default --properties spark.executor.instances=4,spark.driver.cores=8,spark.executor.cores=8,spark.app.name=loan_etl_pipeline \
	--jars gs://${CODE_BUCKET}/dependencies/${DELTA_JAR_FILE},gs://${CODE_BUCKET}/dependencies/delta-storage-2.2.0.jar \
	--metastore-service=projects/${PROJECT_ID}/locations/${REGION}/services/data-catalog-${PROJECT_ID} \
	--version=2.0 \
	-- --project=${PROJECT_ID} --bucket-name=${DATA_BUCKET} --source-prefix=SME/bronze/amortisation --target-prefix=SME/silver/amortisation --pcds="" --stage-name=silver_amortisation
