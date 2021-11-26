[]: # =============
[]: # This file is automatically generated from the templates in stackabletech/operator-templating
[]: # DON'T MANUALLY EDIT THIS FILE
[]: # =============

# Helm Chart for Stackable Operator for Stackable Restart Controller

This Helm Chart can be used to install Custom Resource Definitions and the Operator for Stackable Restart Controller provided by Stackable.


## Requirements

- Create a [Kubernetes Cluster](../Readme.md)
- Install [Helm](https://helm.sh/docs/intro/install/)


## Install the Stackble Operator for Stackable Restart Controller

```bash
# From the root of the operator repository
make compile-chart

helm install restart-controller-operator deploy/helm/restart-controller-operator
```




## Create a Stackable Restart Controller Cluster

as described [here](https://docs.stackable.tech/restart-controller/index.html)



The operator has example requests included in the [`/examples`](https://github.com/stackabletech/restart-controller/operator/tree/main/examples) directory that can be used to spin up a cluster.


## Links

https://github.com/stackabletech/restart-controller-operator

