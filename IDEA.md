I want to build a tool that can help me collect data over long periods of time. Specifically from an API that i can call periodically. This tool will take in a config and it will read from
the config to spawn a set of tasks. At the beginning it will be a a few APIs only and maybe even a good idea to hard code shapes and intervals but let me explain abit more

An example API that i want to collect is using [nea-rs](https://github.com/InfiniteUnion/nea-rs), basically just weather data. Each API has its own interval where it gets refreshed.
This particular API is for singapore weather data and the goal is to store data collected to somewhere long term, for example huggingface. Whatever we do with the data later on is up to the user, eg data analytics.

I believe we can have some general idea of a Sink which can be later extended to other kinds of sink but we can start off with HuggingFace. So the whole idea is quite simple

- Spawn task that runs at interval
- Query API
- Transform data to fit Sink
- Sink ingest data

Also in order for us not to spam the provider we can like buffer things into 1 hour blocks or x block whatever and the program should be fault tolerant in someway so like if it crashes due to some reason (k8s related shit for example)
it will flush the buffer. ALso we can incorporate some other thinsg to make it fault tolerant like if the task panic we respawn or something like that.

The program should be able to handle multiple kinds of collectors and DX wise IT IS OK for the user to impl custom adaptor traits, dont have to just read some yaml and be dynamic. The program is like a base where they can build on top of,
kinda like how tower in rust is very nicely abstracted out.

I might have missed out somethings lets plan together
