# fast-fuzzy

no gil testing

docker run -it --rm -e PYTHON_GIL=0 -v ./index:/code/index --network=musicbrainz-docker_default fast-fuzzy-nogil python ./fuzzy_index.py build
