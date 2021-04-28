# Move to project directory
cd App/

# Build Maven project
mvn package

# Build Docker image from Dockerfile
docker build -t test .

# Run process binding folder in host to folder in container
docker run -it \
-v /Users/goncalo/Documents/University/GGCD/Classes/Data:/data \
test /data/title.basics.tsv.bz2 /data/title.principals.tsv.bz2