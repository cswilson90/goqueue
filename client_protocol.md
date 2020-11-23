# Client Protocol

This document outlines the protocol that should be used by clients wishing to use the Go Job Queue server.

## Data Definitions

Please note all integers should be encoded in little-endian format.

* `<string>` - A generic UTF8 encoded string terminated by a null byte.
* `<queue>` - A `<string>` representing the name of a queue on the server.
* `<status>` - A `<string>` representing the status of a job.
* `<id>` - A 64 bit unsigned integer representing the ID of a job in the queue.
* `<priority>` - A 32 bit unsigned integer representing the priority of a job in the queue.
* `<ttp>`- (Time To Process) A 32 bit unsigned int representing the number of seconds after which
    a reserved job will be released back in to the ready state for another worker to reserve.
* `<data>` - The data for a job. The first 4 bytes of the data should be an unsigned
    integer giving the length of the rest of the data in bytes.
* `<timeout>` - A 32 bit unsigned int representing the number of seconds to wait before giving
    up on a command. A timeout of 0 sets an unlimited timeout.
* `<\0>` - A null byte.

## Error Responses

All responses to commands can return an error message instead of the successful
responses specified below.

### Error

Returned if an error occurs. The returned string will describe the error.

Response: `ERROR<\0><string>`

## Commands

The following commands are recognised by the server.

### Add

Adds a job to the queue with the given queue name. The response returns the ID
of the newly added job.

Client: `ADD<\0><queue><priority><ttp><data>`

Response: `ADDED<\0><id>`

### Connect

A no-op command to establish a connection to the server.

Client: `CONNECT<\0>`

Response: `OK<\0>`

### Delete

Deletes the job with the given ID from the queue.

Client: `DELETE<\0><id>`

Response: `OK<\0>`

### Reserve

Reserves a job from the queue. If successful the response includes all data for the
job. If the timeout expires before a job can be reserved then the server responds
with a timeout response.

Client `RESERVE</0><queue><timeout>`

Successful Response: `RESERVED<\0><id><priority><ttp><status><data>`

Timeout Response: `TIMEOUT<\0>`

