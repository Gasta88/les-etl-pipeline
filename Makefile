PROJECT_ID ?= dataops-369610
REGION ?= europe-west3
PROJECT_NUMBER ?= $$(gcloud projects list --filter=${PROJECT_ID} --format="value(PROJECT_NUMBER)")
CODE_BUCKET ?= data-lake-code-${PROJECT_NUMBER}
RAW_BUCKET ?= algoritmica_data
DATA_BUCKET ?= fgasta_data_lake_test
PHS_BUCKET ?= spark-hist-repo-${PROJECT_NUMBER}
APP_NAME ?= $$(cat pyproject.toml| grep name | cut -d" " -f3 | sed  's/"//g')
VERSION_NO ?= $$(poetry version --short)
SRC_WITH_DEPS ?= src_with_deps
DELTA_JAR_FILE ?= delta-core_2.13-2.1.0.jar
# ED_CODE ?= SMEMBE000095100220092
ED_CODE ?= SMESES000176100320040

.PHONY: $(shell sed -n -e '/^$$/ { n ; /^[^ .\#][^ ]*:/ { s/:.*$$// ; p ; } ; }' $(MAKEFILE_LIST))

.DEFAULT_GOAL := help

help: ## This is help
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

setup: ## Setup Buckets and Dataset for Demo
	@echo "Project=${PROJECT_ID}--${PROJECT_NUMBER}--${CODE_BUCKET}"
	@gsutil mb -c standard -l ${REGION} -p ${PROJECT_ID} gs://${CODE_BUCKET}
	@echo "The Following Buckets created - ${CODE_BUCKET},${DATA_BUCKET}"
	@echo "Create Hive Metastore"
	@gcloud metastore services create data-catalog-${PROJECT_ID} --hive-metastore-version=3.1.2 --location=${REGION}

setup_phs: ## Setup Persisten (spark) History Server
	@echo "Project=${PROJECT_ID}--${PROJECT_NUMBER}--${PHS_BUCKET}"
	@gsutil mb -c standard -l ${REGION} -p ${PROJECT_ID} gs://${PHS_BUCKET}
	@echo "The Following Buckets created - ${PHS_BUCKET}"
	@gcloud dataproc clusters create spark-hist-srv-${PROJECT_ID} --region=${REGION} --single-node --enable-component-gateway \
    --properties=spark:spark.history.fs.logDirectory=gs://${PHS_BUCKET}/*/spark-job-history \
    --properties=yarn:yarn.nodemanager.remote-app-log-dir=gs://${PHS_BUCKET}/*/yarn-logs \
    --properties=mapred:mapreduce.jobhistory.read-only.dir-pattern=gs://${PHS_BUCKET}/*/mapreduce-job-history/done \

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


run_asset_profile_bronze: ## Run the dataproc serverless job
	# @gcloud compute networks subnets update default \
	# --region=${REGION} \
	# --enable-private-ip-google-access
	@gcloud dataproc batches submit --project ${PROJECT_ID} --region ${REGION} pyspark \
	gs://${CODE_BUCKET}/dist/main.py --py-files=gs://${CODE_BUCKET}/dist/${APP_NAME}_${VERSION_NO}.zip \
	--subnet default --properties spark.executor.instances=4,spark.driver.cores=8,spark.executor.cores=8,spark.executor.memory=16g,spark.app.name=loan_etl_pipeline \
	--jars gs://${CODE_BUCKET}/dependencies/${DELTA_JAR_FILE},gs://${CODE_BUCKET}/dependencies/delta-storage-2.2.0.jar \
	--metastore-service=projects/${PROJECT_ID}/locations/${REGION}/services/data-catalog-${PROJECT_ID} \
	--history-server-cluster=projects/${PROJECT_ID}/regions/${REGION}/clusters/spark-hist-srv-${PROJECT_ID} \
	--version=2.0 \
	-- --project=${PROJECT_ID} --raw-bucketname=${RAW_BUCKET} --data-bucketname=${DATA_BUCKET} --source-prefix=mini_source/${ED_CODE} --file-key=Loan_Data --stage-name=profile_bronze_asset &
run_collateral_profile_bronze: ## Run the dataproc serverless job
	# @gcloud compute networks subnets update default \
	# --region=${REGION} \
	# --enable-private-ip-google-access
	@gcloud dataproc batches submit --project ${PROJECT_ID} --region ${REGION} pyspark \
	gs://${CODE_BUCKET}/dist/main.py --py-files=gs://${CODE_BUCKET}/dist/${APP_NAME}_${VERSION_NO}.zip \
	--subnet default --properties spark.executor.instances=4,spark.driver.cores=8,spark.executor.cores=8,spark.executor.memory=16g,spark.app.name=loan_etl_pipeline \
	--jars gs://${CODE_BUCKET}/dependencies/${DELTA_JAR_FILE},gs://${CODE_BUCKET}/dependencies/delta-storage-2.2.0.jar \
	--metastore-service=projects/${PROJECT_ID}/locations/${REGION}/services/data-catalog-${PROJECT_ID} \
	--history-server-cluster=projects/${PROJECT_ID}/regions/${REGION}/clusters/spark-hist-srv-${PROJECT_ID} \
	--version=2.0 \
	-- --project=${PROJECT_ID} --raw-bucketname=${RAW_BUCKET} --data-bucketname=${DATA_BUCKET} --source-prefix=mini_source/${ED_CODE} --file-key=Collateral --stage-name=profile_bronze_collateral &

run_bond_info_profile_bronze: ## Run the dataproc serverless job
	# @gcloud compute networks subnets update default \
	# --region=${REGION} \
	# --enable-private-ip-google-access
	@gcloud dataproc batches submit --project ${PROJECT_ID} --region ${REGION} pyspark \
	gs://${CODE_BUCKET}/dist/main.py --py-files=gs://${CODE_BUCKET}/dist/${APP_NAME}_${VERSION_NO}.zip \
	--subnet default --properties spark.executor.instances=4,spark.driver.cores=8,spark.executor.cores=8,spark.executor.memory=16g,spark.app.name=loan_etl_pipeline \
	--jars gs://${CODE_BUCKET}/dependencies/${DELTA_JAR_FILE},gs://${CODE_BUCKET}/dependencies/delta-storage-2.2.0.jar \
	--metastore-service=projects/${PROJECT_ID}/locations/${REGION}/services/data-catalog-${PROJECT_ID} \
	--history-server-cluster=projects/${PROJECT_ID}/regions/${REGION}/clusters/spark-hist-srv-${PROJECT_ID} \
	--version=2.0 \
	-- --project=${PROJECT_ID} --raw-bucketname=${RAW_BUCKET} --data-bucketname=${DATA_BUCKET} --source-prefix=mini_source/${ED_CODE} --file-key=Bond_Info --stage-name=profile_bronze_bond_info &

# run_amortisation_profile_bronze: ## Run the dataproc serverless job
# 	# @gcloud compute networks subnets update default \
# 	# --region=${REGION} \
# 	# --enable-private-ip-google-access
# 	@gcloud dataproc batches submit --project ${PROJECT_ID} --region ${REGION} pyspark \
# 	gs://${CODE_BUCKET}/dist/main.py --py-files=gs://${CODE_BUCKET}/dist/${APP_NAME}_${VERSION_NO}.zip \
# 	--subnet default --properties spark.executor.instances=4,spark.driver.cores=8,spark.executor.cores=16,spark.executor.memory=64g,spark.app.name=loan_etl_pipeline \
# 	--jars gs://${CODE_BUCKET}/dependencies/${DELTA_JAR_FILE},gs://${CODE_BUCKET}/dependencies/delta-storage-2.2.0.jar \
# 	--metastore-service=projects/${PROJECT_ID}/locations/${REGION}/services/data-catalog-${PROJECT_ID} \
# 	--history-server-cluster=projects/${PROJECT_ID}/regions/${REGION}/clusters/spark-hist-srv-${PROJECT_ID} \
# 	--version=2.0 \
# 	-- --project=${PROJECT_ID} --raw-bucketname=${RAW_BUCKET} --data-bucketname=${DATA_BUCKET} --source-prefix=mini_source/${ED_CODE} --file-key=Amortization --stage-name=profile_bronze_amortisation &


# all_bronze_profile: run_asset_profile_bronze run_amortisation_profile_bronze run_bond_info_profile_bronze run_collateral_profile_bronze
# 	@echo "All targets for profiling bronze layer have been run"
all_bronze_profile: run_asset_profile_bronze run_bond_info_profile_bronze run_collateral_profile_bronze
	@echo "All targets for profiling bronze layer have been run"

#-------------------------------------------------------------------------------------------
run_asset_bronze: ## Run the dataproc serverless job
	# @gcloud compute networks subnets update default \
	# --region=${REGION} \
	# --enable-private-ip-google-access
	@gcloud dataproc batches submit --project ${PROJECT_ID} --region ${REGION} pyspark \
	gs://${CODE_BUCKET}/dist/main.py --py-files=gs://${CODE_BUCKET}/dist/${APP_NAME}_${VERSION_NO}.zip \
	--subnet default --properties spark.executor.instances=4,spark.driver.cores=8,spark.executor.cores=8,spark.executor.memory=16g,spark.app.name=loan_etl_pipeline \
	--jars gs://${CODE_BUCKET}/dependencies/${DELTA_JAR_FILE},gs://${CODE_BUCKET}/dependencies/delta-storage-2.2.0.jar \
	--metastore-service=projects/${PROJECT_ID}/locations/${REGION}/services/data-catalog-${PROJECT_ID} \
	--history-server-cluster=projects/${PROJECT_ID}/regions/${REGION}/clusters/spark-hist-srv-${PROJECT_ID} \
	--version=2.0 \
	-- --project=${PROJECT_ID} --raw-bucketname=${RAW_BUCKET} --data-bucketname=${DATA_BUCKET} --source-prefix=mini_source/${ED_CODE} --target-prefix=SME/bronze/assets --file-key=Loan_Data --stage-name=bronze_asset &

run_collateral_bronze: ## Run the dataproc serverless job
	# @gcloud compute networks subnets update default \
	# --region=${REGION} \
	# --enable-private-ip-google-access
	@gcloud dataproc batches submit --project ${PROJECT_ID} --region ${REGION} pyspark \
	gs://${CODE_BUCKET}/dist/main.py --py-files=gs://${CODE_BUCKET}/dist/${APP_NAME}_${VERSION_NO}.zip \
	--subnet default --properties spark.executor.instances=4,spark.driver.cores=8,spark.executor.cores=8,spark.executor.memory=16g,spark.app.name=loan_etl_pipeline \
	--jars gs://${CODE_BUCKET}/dependencies/${DELTA_JAR_FILE},gs://${CODE_BUCKET}/dependencies/delta-storage-2.2.0.jar \
	--metastore-service=projects/${PROJECT_ID}/locations/${REGION}/services/data-catalog-${PROJECT_ID} \
	--history-server-cluster=projects/${PROJECT_ID}/regions/${REGION}/clusters/spark-hist-srv-${PROJECT_ID} \
	--version=2.0 \
	-- --project=${PROJECT_ID} --raw-bucketname=${RAW_BUCKET} --data-bucketname=${DATA_BUCKET} --source-prefix=mini_source/${ED_CODE} --target-prefix=SME/bronze/collaterals --file-key=Collateral --stage-name=bronze_collateral &

run_bond_info_bronze: ## Run the dataproc serverless job
	# @gcloud compute networks subnets update default \
	# --region=${REGION} \
	# --enable-private-ip-google-access
	@gcloud dataproc batches submit --project ${PROJECT_ID} --region ${REGION} pyspark \
	gs://${CODE_BUCKET}/dist/main.py --py-files=gs://${CODE_BUCKET}/dist/${APP_NAME}_${VERSION_NO}.zip \
	--subnet default --properties spark.executor.instances=4,spark.driver.cores=8,spark.executor.cores=8,spark.executor.memory=16g,spark.app.name=loan_etl_pipeline \
	--jars gs://${CODE_BUCKET}/dependencies/${DELTA_JAR_FILE},gs://${CODE_BUCKET}/dependencies/delta-storage-2.2.0.jar \
	--metastore-service=projects/${PROJECT_ID}/locations/${REGION}/services/data-catalog-${PROJECT_ID} \
	--history-server-cluster=projects/${PROJECT_ID}/regions/${REGION}/clusters/spark-hist-srv-${PROJECT_ID} \
	--version=2.0 \
	-- --project=${PROJECT_ID} --raw-bucketname=${RAW_BUCKET} --data-bucketname=${DATA_BUCKET} --source-prefix=mini_source/${ED_CODE} --target-prefix=SME/bronze/bond_info --file-key=Bond_Info --stage-name=bronze_bond_info &

# run_amortisation_bronze: ## Run the dataproc serverless job
# 	# @gcloud compute networks subnets update default \
# 	# --region=${REGION} \
# 	# --enable-private-ip-google-access
# 	@gcloud dataproc batches submit --project ${PROJECT_ID} --region ${REGION} pyspark \
# 	gs://${CODE_BUCKET}/dist/main.py --py-files=gs://${CODE_BUCKET}/dist/${APP_NAME}_${VERSION_NO}.zip \
# 	--subnet default --properties spark.executor.instances=4,spark.driver.cores=8,spark.executor.cores=16,spark.executor.memory=64g,spark.app.name=loan_etl_pipeline \
# 	--jars gs://${CODE_BUCKET}/dependencies/${DELTA_JAR_FILE},gs://${CODE_BUCKET}/dependencies/delta-storage-2.2.0.jar \
# 	--metastore-service=projects/${PROJECT_ID}/locations/${REGION}/services/data-catalog-${PROJECT_ID} \
# 	--history-server-cluster=projects/${PROJECT_ID}/regions/${REGION}/clusters/spark-hist-srv-${PROJECT_ID} \
# 	--version=2.0 \
# 	-- --project=${PROJECT_ID} --raw-bucketname=${RAW_BUCKET} --data-bucketname=${DATA_BUCKET} --source-prefix=mini_source/${ED_CODE} --target-prefix=SME/bronze/amortisation --file-key=Amortization --stage-name=bronze_amortisation &

run_deal_details_bronze: ## Run the dataproc serverless job
	# @gcloud compute networks subnets update default \
	# --region=${REGION} \
	# --enable-private-ip-google-access
	@gcloud dataproc batches submit --project ${PROJECT_ID} --region ${REGION} pyspark \
	gs://${CODE_BUCKET}/dist/main.py --py-files=gs://${CODE_BUCKET}/dist/${APP_NAME}_${VERSION_NO}.zip \
	--subnet default --properties spark.executor.instances=4,spark.driver.cores=8,spark.executor.cores=8,spark.executor.memory=16g,spark.app.name=loan_etl_pipeline \
	--jars gs://${CODE_BUCKET}/dependencies/${DELTA_JAR_FILE},gs://${CODE_BUCKET}/dependencies/delta-storage-2.2.0.jar \
	--metastore-service=projects/${PROJECT_ID}/locations/${REGION}/services/data-catalog-${PROJECT_ID} \
	--history-server-cluster=projects/${PROJECT_ID}/regions/${REGION}/clusters/spark-hist-srv-${PROJECT_ID} \
	--version=2.0 \
	-- --project=${PROJECT_ID} --raw-bucketname=${RAW_BUCKET} --data-bucketname=${DATA_BUCKET} --source-prefix=mini_source/${ED_CODE} --target-prefix=SME/bronze/deal_details --file-key=Deal_Details --stage-name=bronze_deal_details &

# all_bronze: run_asset_bronze run_amortisation_bronze run_bond_info_bronze run_collateral_bronze run_deal_details_bronze
# 	@echo "All targets for generating bronze layer have been run"
all_bronze: run_asset_bronze run_bond_info_bronze run_collateral_bronze run_deal_details_bronze
	@echo "All targets for generating bronze layer have been run"

#-------------------------------------------------------------------------------------------

run_asset_silver: ## Run the dataproc serverless job
	# @gcloud compute networks subnets update default \
	# --region=${REGION} \
	# --enable-private-ip-google-access
	@gcloud dataproc batches submit --project ${PROJECT_ID} --region ${REGION} pyspark \
	gs://${CODE_BUCKET}/dist/main.py --py-files=gs://${CODE_BUCKET}/dist/${APP_NAME}_${VERSION_NO}.zip \
	--subnet default --properties spark.executor.instances=4,spark.driver.cores=8,spark.executor.cores=8,spark.executor.memory=16g,spark.app.name=loan_etl_pipeline \
	--jars gs://${CODE_BUCKET}/dependencies/${DELTA_JAR_FILE},gs://${CODE_BUCKET}/dependencies/delta-storage-2.2.0.jar \
	--metastore-service=projects/${PROJECT_ID}/locations/${REGION}/services/data-catalog-${PROJECT_ID} \
	--history-server-cluster=projects/${PROJECT_ID}/regions/${REGION}/clusters/spark-hist-srv-${PROJECT_ID} \
	--version=2.0 \
	-- --project=${PROJECT_ID} --raw-bucketname=${RAW_BUCKET} --data-bucketname=${DATA_BUCKET} --source-prefix=SME/bronze/assets --target-prefix=SME/silver/assets --ed-code=${ED_CODE} --stage-name=silver_asset &

run_collateral_silver: ## Run the dataproc serverless job
	# @gcloud compute networks subnets update default \
	# --region=${REGION} \
	# --enable-private-ip-google-access
	@gcloud dataproc batches submit --project ${PROJECT_ID} --region ${REGION} pyspark \
	gs://${CODE_BUCKET}/dist/main.py --py-files=gs://${CODE_BUCKET}/dist/${APP_NAME}_${VERSION_NO}.zip \
	--subnet default --properties spark.executor.instances=4,spark.driver.cores=8,spark.executor.cores=8,spark.executor.memory=16g,spark.app.name=loan_etl_pipeline \
	--jars gs://${CODE_BUCKET}/dependencies/${DELTA_JAR_FILE},gs://${CODE_BUCKET}/dependencies/delta-storage-2.2.0.jar \
	--metastore-service=projects/${PROJECT_ID}/locations/${REGION}/services/data-catalog-${PROJECT_ID} \
	--history-server-cluster=projects/${PROJECT_ID}/regions/${REGION}/clusters/spark-hist-srv-${PROJECT_ID} \
	--version=2.0 \
	-- --project=${PROJECT_ID} --raw-bucketname=${RAW_BUCKET} --data-bucketname=${DATA_BUCKET} --source-prefix=SME/bronze/collaterals --target-prefix=SME/silver/collaterals --ed-code=${ED_CODE} --stage-name=silver_collateral &

run_bond_info_silver: ## Run the dataproc serverless job
	# @gcloud compute networks subnets update default \
	# --region=${REGION} \
	# --enable-private-ip-google-access
	@gcloud dataproc batches submit --project ${PROJECT_ID} --region ${REGION} pyspark \
	gs://${CODE_BUCKET}/dist/main.py --py-files=gs://${CODE_BUCKET}/dist/${APP_NAME}_${VERSION_NO}.zip \
	--subnet default --properties spark.executor.instances=4,spark.driver.cores=8,spark.executor.cores=8,spark.executor.memory=16g,spark.app.name=loan_etl_pipeline \
	--jars gs://${CODE_BUCKET}/dependencies/${DELTA_JAR_FILE},gs://${CODE_BUCKET}/dependencies/delta-storage-2.2.0.jar \
	--metastore-service=projects/${PROJECT_ID}/locations/${REGION}/services/data-catalog-${PROJECT_ID} \
	--history-server-cluster=projects/${PROJECT_ID}/regions/${REGION}/clusters/spark-hist-srv-${PROJECT_ID} \
	--version=2.0 \
	-- --project=${PROJECT_ID} --raw-bucketname=${RAW_BUCKET} --data-bucketname=${DATA_BUCKET} --source-prefix=SME/bronze/bond_info --target-prefix=SME/silver/bond_info --ed-code=${ED_CODE} --stage-name=silver_bond_info &

# run_amortisation_silver: ## Run the dataproc serverless job
# 	# @gcloud compute networks subnets update default \
# 	# --region=${REGION} \
# 	# --enable-private-ip-google-access
# 	@gcloud dataproc batches submit --project ${PROJECT_ID} --region ${REGION} pyspark \
# 	gs://${CODE_BUCKET}/dist/main.py --py-files=gs://${CODE_BUCKET}/dist/${APP_NAME}_${VERSION_NO}.zip \
# 	--subnet default --properties spark.executor.instances=4,spark.driver.cores=8,spark.executor.cores=16,spark.executor.memory=64g,spark.app.name=loan_etl_pipeline \
# 	--jars gs://${CODE_BUCKET}/dependencies/${DELTA_JAR_FILE},gs://${CODE_BUCKET}/dependencies/delta-storage-2.2.0.jar \
# 	--metastore-service=projects/${PROJECT_ID}/locations/${REGION}/services/data-catalog-${PROJECT_ID} \
# 	--history-server-cluster=projects/${PROJECT_ID}/regions/${REGION}/clusters/spark-hist-srv-${PROJECT_ID} \
# 	--version=2.0 \
# 	-- --project=${PROJECT_ID} --raw-bucketname=${RAW_BUCKET} --data-bucketname=${DATA_BUCKET} --source-prefix=SME/bronze/amortisation --target-prefix=SME/silver/amortisation --ed-code=${ED_CODE} --stage-name=silver_amortisation &

run_deal_details_silver: ## Run the dataproc serverless job
	# @gcloud compute networks subnets update default \
	# --region=${REGION} \
	# --enable-private-ip-google-access
	@gcloud dataproc batches submit --project ${PROJECT_ID} --region ${REGION} pyspark \
	gs://${CODE_BUCKET}/dist/main.py --py-files=gs://${CODE_BUCKET}/dist/${APP_NAME}_${VERSION_NO}.zip \
	--subnet default --properties spark.executor.instances=4,spark.driver.cores=8,spark.executor.cores=8,spark.executor.memory=16g,spark.app.name=loan_etl_pipeline \
	--jars gs://${CODE_BUCKET}/dependencies/${DELTA_JAR_FILE},gs://${CODE_BUCKET}/dependencies/delta-storage-2.2.0.jar \
	--metastore-service=projects/${PROJECT_ID}/locations/${REGION}/services/data-catalog-${PROJECT_ID} \
	--history-server-cluster=projects/${PROJECT_ID}/regions/${REGION}/clusters/spark-hist-srv-${PROJECT_ID} \
	--version=2.0 \
	-- --project=${PROJECT_ID} --raw-bucketname=${RAW_BUCKET} --data-bucketname=${DATA_BUCKET} --source-prefix=SME/bronze/deal_details --target-prefix=SME/silver/deal_details --stage-name=silver_deal_details &

# all_silver: run_asset_silver run_amortisation_silver run_bond_info_silver run_collateral_silver run_deal_details_silver
# 	@echo "All targets for generating silver layer have been run"
all_silver: run_asset_silver run_bond_info_silver run_collateral_silver run_deal_details_silver
	@echo "All targets for generating silver layer have been run"

run_quandl_silver: ## Run the dataproc serverless job
	# @gcloud compute networks subnets update default \
	# --region=${REGION} \
	# --enable-private-ip-google-access
	@gcloud dataproc batches submit --project ${PROJECT_ID} --region ${REGION} pyspark \
	gs://${CODE_BUCKET}/dist/main.py --py-files=gs://${CODE_BUCKET}/dist/${APP_NAME}_${VERSION_NO}.zip \
	--subnet default --properties spark.executor.instances=4,spark.driver.cores=8,spark.executor.cores=8,spark.executor.memory=16g,spark.app.name=loan_etl_pipeline \
	--jars gs://${CODE_BUCKET}/dependencies/${DELTA_JAR_FILE},gs://${CODE_BUCKET}/dependencies/delta-storage-2.2.0.jar,gs://${CODE_BUCKET}/dependencies/spark-3.1-bigquery-0.28.0-preview.jar \
	--metastore-service=projects/${PROJECT_ID}/locations/${REGION}/services/data-catalog-${PROJECT_ID} \
	--history-server-cluster=projects/${PROJECT_ID}/regions/${REGION}/clusters/spark-hist-srv-${PROJECT_ID} \
	--version=2.0 \
	-- --project=${PROJECT_ID} --raw-bucketname=${RAW_BUCKET} --data-bucketname=${DATA_BUCKET} --source-prefix=externals/quandl --stage-name=silver_quandl &