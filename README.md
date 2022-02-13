# oereb-db
Docker image providing a PostGIS database with OEREB tables

...

## Usage

```
docker run --rm --name oerebdb -p 54323:5432 -e POSTGRES_PASSWORD=mysecretpassword -e POSTGRES_DB=oereb -e POSTGRES_HOST_AUTH_METHOD=md5 -e PG_READ_PWD=dmluser -e PG_WRITE_PWD=ddluser -e PG_GRETL_PWD=gretl edigonzales/oereb-db:2
```

```
mkdir -m 0777 ~/pgdata-oereb
docker run --rm --name oerebdb -p 54323:5432 -v ~/pgdata-oereb:/var/lib/postgresql/data:delegated -e POSTGRES_PASSWORD=mysecretpassword -e POSTGRES_DB=oereb -e POSTGRES_HOST_AUTH_METHOD=md5 -e PG_READ_PWD=dmluser -e PG_WRITE_PWD=ddluser -e PG_GRETL_PWD=gretl edigonzales/oereb-db:2
```

## Develop
Java muss installiert sein.

```
jbang edit create_schema_sql.java
```

Auf Apple Silicon ist _vscodium_ noch nicht verfügbar. Aus diesem Grund muss als Editor _VS Code_ verwendet werden. Damit der Aufruf via Console funktioniert, muss in `.zshrc` der `PATH` angepasst werden:

```
export PATH="$PATH:/Users/stefan/apps/vscode/Visual Studio Code.app/Contents/Resources/app/bin"
```

Der der Aufruf zum Editieren ist wie folgt:

```
jbang edit --open=code create_schema_sql.java
```

Code ausführen:

```
jbang create_schema_sql.java
```

Falls jbang nicht installiert ist:

curl -Ls https://sh.jbang.dev | bash -s - create_schema_sql.java

### Build

```
docker build -t edigonzales/oereb-db:2 .
```
