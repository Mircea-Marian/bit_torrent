# bit_torrent

I implemented a torrent-type system. The server keeps in its memory details about where each fragment of a file can be found. A client can retrieve a file and send a fragment to another client. Both the client and the server are multithreaded.

They were both implemented using Java non-blocking socket channels.
