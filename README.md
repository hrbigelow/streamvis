# streamvis - interactive visualizations of streaming data with Bokeh

This is a small repo that allows you to design custom visualizations using the Bokeh
library, and define how they update with periodic arrivals of new data.

# Setup

```bash
# launch the REST server (test with rest-server-test.sh)
# or, launch on a separate machine
REST_HOST=localhost
REST_PORT=8080
python rest-server.py $REST_PORT &

# launch the bokeh server
bokeh serve my-app-server.py $REST_HOST $REST_PORT $APP_NAME &

# start your data-producing application
python my-app-client.py $REST_HOST $REST_PORT $APP_NAME &

# watch your data at localhost:5006/my-app-server
```

# 
