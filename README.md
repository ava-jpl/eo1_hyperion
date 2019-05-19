## EO1_Hyperion
Ingests EO1_Hyperion Products
----
There is 1 associated jobs:
- Ingest - EO1_Hyperion

### Ingest - EO1_Hyperion
-----
Job is of type iteration. It takes in an input MET-EO1__Hyperion product, along with a user supplied product level. The job localizes and publishes the associated product if the product level exists on the USGS, and if the product does not exist on GRQ. Browse products will also be generated.

Product levels are: L1R, L1T, L1Gst

AST_Hyperion product spec is the followingc:

    AST_Hyperion-<product_format>-<sensing_start_datetime>_<sensing_end_datetime>-<version_number>

