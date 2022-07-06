# Results Report
### Logical cores by running multiprocessing.cpu_count(): 4
## Load users (insert_one)
time: 1.7046680450439453 second(s)

## Load status updates (insert_one)
time: 427.97299313545227 second(s)

## Load users in chunks (insert_many)
chunk size 10 time: 1.06899094581604 second(s)
chunk size 50 time: 0.41586780548095703 second(s)
chunk size 100 time: 0.383333683013916 second(s)
chunk size 500 time: 0.3166818618774414 second(s)
chunk size 1000 time: 0.28677892684936523 second(s)

## Load status updates in chunks (insert_many)
chunk size 100 time: 30.5253849029541 second(s)
chunk size 500 time: 23.905611038208008 second(s)
chunk size 1000 time: 17.001656770706177 second(s)
chunk size 5000 time: 20.34459900856018 second(s)
chunk size 10000 time: 20.66141390800476 second(s)

## Load users multiprocess (insert_one)
chunk size 100 time: 9.57846188545227 second(s)
chunk size 500 time: 4.80892014503479 second(s)
chunk size 1000 time: 4.169945955276489 second(s)

### Load status updates multiprocess (insert_one)
chunk size 100 time: 1074.3979320526123 second(s)
chunk size 1000 time: 292.4766447544098 second(s)
chunk size 5000 time: 224.4487829208374 second(s)
chunk size 10000 time: 229.53007888793945 second(s)
chunk size 50000 time: 237.1156153678894 second(s)