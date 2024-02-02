cd images/mock-collector
docker build . -t aws-appsignals-mock-collector-python
cd ..

cd applications
for dir in */
do
  cd $dir
  docker build . -t aws-appsignals-tests-${dir%/}-app
  cd ..
done

cd ../..