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

mkdir dist
rm -rf dist/*
cd images/mock-collector
python3 -m build --outdir ../../dist --no-isolation
cd ../../dist
pip wheel --no-deps mock_collector-1.0.0.tar.gz
pip install mock_collector-1.0.0-py3-none-any.whl --force-reinstall

cd ../tests
python3 -m build --outdir ../dist --no-isolation
cd ../dist
pip wheel --no-deps contract_tests-1.0.0.tar.gz
# --force-reinstall causes `ERROR: No matching distribution found for mock-collector==1.0.0`, but uninstalling and reinstalling works pretty reliably.
pip uninstall contract-tests -y
pip install contract_tests-1.0.0-py3-none-any.whl

cd ..