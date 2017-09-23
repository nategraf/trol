# Redis Thin  Object Layer (RTOL)
**A library for fast, predictable, and human readable python object mappings to Redis**

### Why?
I wanted an object oriented way to interact with redis that would provide exacting control over database layout, predicatble and fast queries, and good documentation. (hopefully I got that last one right, but I'm not the one to judge)

The first goal of rtol is a statically defined, human-readble database structure defined my python classes. This allows the dev to look at the database at runtime and read it as easily as the code which defined it. The dev should be able to modify the database and know exactly what effect it will have on the program. As a result of this, rtol explicitly does not provide indexing or store supporting datastructures not defined by the programer.

The second goal of rtol is fast and predictable querying. Any python access, function, or modification should result and in one or zero commands sent to the database. One result of this is a structure which encourages the dev to create a database where eveything is defined in location and uniquely identifieable without searching. 

## How do I use it?
Well it doesn't work yet... so don't

But if you are actally reading this, I appreciate the enthusism! Shoot me an email at nategraf1@gmail.com so I know someone cares about this project!

