#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdarg.h>
#include <string.h>
#include <unistd.h>	// STDOUT_FILENO
#include <zmq.h>

#include "stdsub.h"

int main(int argc, char **argv)
{
    uint64_t nlines = 0;        // number read and emitted
    uint64_t seqno = 0;         // message sequence/serial no. for detection of dropped messages
    uint64_t last_seqno = 0;    // last seq/serial no. seen
    int more;                   // bool multipart
    size_t more_size = sizeof(more);

    size_t buflen = 1048576;
    char *line = malloc(buflen);
    ssize_t linelen = 0;

    int opt;
    int send_eot = 0;   // option: propagate EOT marker byte (but don't close stdout)
    int term_at_eot = 0;// option: close stdout and terminate if EOT recv'd
    while ((opt = getopt(argc, argv, "et")) != -1) {
        switch (opt) {
            case 'e': send_eot = 1; break;
            case 't': term_at_eot = 1; break;
            default:
                fprintf(stderr, "Usage: %s [-e] [-t] [socket addr]\tDefault socket address: %s\n", argv[0], addr);
                exit(EXIT_FAILURE);
        }
    }
    // Now optind (declared extern int by <unistd.h>) is the index of the first non-option argument.
    // If it is >= argc, there were no non-option arguments.
    if (optind < argc)
        addr = argv[optind];
    
    int major, minor, patch;
    zmq_version(&major, &minor, &patch);
    jlog("ZMQ version: %d.%d.%d", major, minor, patch);   
    
    jlog("Establishing ZMQ SUB socket at address: %s", addr);
    void *context = zmq_ctx_new();
    void *subscriber = zmq_socket(context, ZMQ_SUB);
    int hwm = 0;    // high water mark on recieve side. default 1000, 0:no limit
    zmq_setsockopt(subscriber, ZMQ_RCVHWM, &hwm, sizeof(hwm) );
    int rc = zmq_connect(subscriber, addr);
    assert(rc == 0);
    // TODO: error handling for rc < 0 http://api.zeromq.org/master:zmq-bind

    // subscribe to all messages
    rc = zmq_setsockopt(subscriber, ZMQ_SUBSCRIBE, "", 0);
    assert(rc == 0);

    jlog("Streaming queue to stdout");
    if (send_eot)       jlog("Option: Propagate EOT marker");
    if (term_at_eot)    jlog("Option: Terminate at EOT marker");
    while (1) {
        int nbytes = zmq_recv(subscriber, &seqno, sizeof(seqno), 0);
        if (!seqno) {
            jlog("EOT (%llu lines received, %llu dropped)", nlines, last_seqno - nlines);
            if (send_eot) {
                line[0] = EOT;  // assignment previously was not required as EOT was recv'd in-band
                line[1] = '\n'; // consumer likely to be line-oriented
                write(STDOUT_FILENO, line, 2);
                fflush(stdout);
            }
            if (term_at_eot) break;
            else {
                last_seqno = 0;
                continue;           // signal only, no message
            }
        }
        else if (seqno > ++last_seqno ) {
            jlog("Missed %llu messages (%llu, %llu)", (seqno - last_seqno), seqno, last_seqno);
            last_seqno = seqno;
        }
        else if (seqno < last_seqno) {
            // this could happen if:
            //  1. we missed an EOT previously
            //  2. logic error mismatch between stdpub/stdsub
            //  3. ???
            jlog("seqno (%llu) unexpectedly less than ++last_seqno (%llu)", seqno, last_seqno);
            last_seqno = seqno;
        }
        rc = zmq_getsockopt(subscriber, ZMQ_RCVMORE, &more, &more_size);
        nbytes = zmq_recv(subscriber, line, buflen, 0);

        if ( nbytes > (buflen >> 1)  )
        {
            if ( nbytes <= buflen )
                jlog("Recieved message greater than 50%% of buffer size (%lu), expanding 2x", buflen);
            else if ( nbytes > buflen )
                jlog("Dropped part of message greater than buffer size (%lu); expanding 2x", buflen);
                // TODO actually drop the entire message, or rewrite line if format known
            else {
                jlog("Logic error");
                exit(EXIT_FAILURE);
            }

            buflen *= 2;
            line = realloc(line, buflen);
            if (line == NULL) {
                jlog("Memory reallocation for buffer size %lu failed", buflen);
                exit(1);
            }
        }

        write(STDOUT_FILENO, line, nbytes); 
            write(STDOUT_FILENO, line, nbytes); 
        write(STDOUT_FILENO, line, nbytes); 
        nlines++;
    }

    jlog("---SUMMARY---");
    jlog("%llu lines processed", nlines);
    jlog("%llu lines dropped", (last_seqno - nlines));

    jlog("ZMQ shutdown");
    zmq_close(subscriber);
    zmq_ctx_destroy(context);

    return 0;
}
