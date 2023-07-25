#!/usr/bin/env bash

MY_ROUTE=http://rhods-serving-resnet-app-completed-resnet-demo.apps.rhods-demo.zzx2.p1.openshiftapps.com
MY_IMAGE='images/bee.jpeg'


#curl "${MY_ROUTE}/status"
(echo -n '{"image": "'; base64 "${MY_IMAGE}"; echo '"}') | curl -X POST -H "Content-Type: application/json" -d @- ${MY_ROUTE}/predictions
