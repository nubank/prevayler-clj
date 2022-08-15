# prevayler-clj-aws

An implementation of [`prevayler-clj`](https://github.com/klauswuestefeld/prevayler-clj) that runs on top of AWS services.

## Usage

Check `prevayler-clj` [usage](https://github.com/klauswuestefeld/prevayler-clj#usage).

See `prevayler-clj-aws.core/prevayler!` docstring for details on how to create a `prevayler-clj-aws` instance.

## AWS setup

`prevayler-clj-aws` requires a Dynamodb table with the following structure:

```edn
{:AttributeDefinitions [{:AttributeName "partkey"
                         :AttributeType "N"}
                        {:AttributeName "order"
                         :AttributeType "N"}]
 :KeySchema [{:AttributeName "partkey"
              :KeyType "HASH"}
             {:AttributeName "order"
              :KeyType "RANGE"}]}
```

`prevayler-aws-clj` also requires a S3 bucket to save the snapshot file.

## How it works

Conceptually `prevayler-clj-aws` works the same as `prevayler-clj` [does](https://github.com/klauswuestefeld/prevayler-clj/blob/master/README.md#how-it-works).

In practice instead of saving the snapshot and journal in the file system,
`prevayler-clj-aws` saves the snapshot in a S3 bucket and the journal in a Dynamodb table.

The snapshot is saved as single file in S3. The file contains the id (partkey) of
the snapshot and the business state. The file is overwritten everytime
a new snapshot is made.

Each item in the Dynamodb table correspond to an event in the journal.
Each item has a primary key composed of a partkey and order.
The partkey corresponds to the id of the snapshot after which the event was created.
The order corresponds to the time the event was created.
Newer events have a bigger order number.
Everytime a new snapshot is made, the order restarts from 1.

## Limitations

* `prevayler-aws-clj` presumes there is only one instance of the application running at a given time.
Running more than one instance can lead to an inconsistent state.
* `prevayler-aws-clj` does not delete the previous journal when generating a snapshot
(which means the entire event log is kept in Dynamodb forever).
It is fine to delete old events, but doing that is left for the user.
