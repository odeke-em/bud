#include <stdio.h>   /* snprintf */
#include <limits.h>  /* INT_MAX */
#include <stdlib.h>  /* malloc, free, NULL */

#include "uv.h"

#include "ipc.h"
#include "common.h"
#include "config.h"
#include "error.h"
#include "logger.h"

typedef struct bud_ipc_msg_handle_s bud_ipc_msg_handle_t;

struct bud_ipc_msg_handle_s {
  bud_ipc_t* ipc;
  uv_tcp_t tcp;
  uv_write_t req;
};

static void bud_ipc_alloc_cb(uv_handle_t* handle,
                             size_t suggested_size,
                             uv_buf_t* buf);
static void bud_ipc_read_cb(uv_stream_t* stream,
                            ssize_t nread,
                            const uv_buf_t* buf);
static void bud_ipc_parse(bud_ipc_t* ipc);
static void bud_ipc_msg_handle_on_close(uv_handle_t* handle);
static void bud_ipc_msg_send_cb(uv_write_t* req, int status);


bud_error_t bud_ipc_init(bud_ipc_t* ipc, bud_config_t* config) {
  int r;
  bud_error_t err;

  ringbuffer_init(&ipc->buffer);

  ipc->handle = malloc(sizeof(*ipc->handle));
  if (ipc->handle == NULL) {
    err = bud_error_str(kBudErrNoMem, "ipc->handle");
    goto failed_alloc_handle;
  }

  r = uv_pipe_init(config->loop, ipc->handle, 1);
  if (r != 0) {
    err = bud_error_num(kBudErrIPCPipeInit, r);
    goto failed_pipe_init;
  }

  ipc->handle->data = ipc;
  ipc->config = config;
  ipc->state = kBudIPCType;
  ipc->waiting = 1;
  ipc->client_cb = NULL;

  return bud_ok();

failed_pipe_init:
  free(ipc);

failed_alloc_handle:
  return err;
}


bud_error_t bud_ipc_open(bud_ipc_t* ipc, uv_file file) {
  int r;

  r = uv_pipe_open(ipc->handle, file);
  if (r != 0)
    return bud_error_num(kBudErrIPCPipeOpen, r);

  return bud_ok();
}


bud_error_t bud_ipc_start(bud_ipc_t* ipc) {
  int r;

  r = uv_read_start((uv_stream_t*) ipc->handle,
                    bud_ipc_alloc_cb,
                    bud_ipc_read_cb);

  bread_crumb_str("IPC: %p r: %d\n", ipc, r);
  if (r != 0)
    return bud_error_num(kBudErrIPCReadStart, r);

  return bud_ok();
}


void bud_ipc_close(bud_ipc_t* ipc) {
  ringbuffer_destroy(&ipc->buffer);
  if (ipc->handle != NULL)
    uv_close((uv_handle_t*) ipc->handle, (uv_close_cb) free);
  ipc->handle = NULL;
}


void bud_ipc_alloc_cb(uv_handle_t* handle,
                      size_t suggested_size,
                      uv_buf_t* buf) {
  bud_ipc_t* ipc;
  size_t avail;
  char* ptr;

  ipc = handle->data;

  avail = 0;
  ptr = ringbuffer_write_ptr(&ipc->buffer, &avail);
  *buf = uv_buf_init(ptr, avail);
}


void bud_ipc_read_cb(uv_stream_t* stream,
                     ssize_t nread,
                     const uv_buf_t* buf) {
  bud_ipc_t* ipc;
  int r;

  bread_crumb();

  /* This should not really happen */
  ASSERT(nread != UV_EOF, "Unexpected EOF on ipc pipe");
  ipc = stream->data;

  /* Error, must close the stream */
  if (nread < 0) {
    uv_close((uv_handle_t*) ipc->handle, (uv_close_cb) free);
    ipc->handle = NULL;
    /* XXX Report error */
    return;
  }

  r = ringbuffer_write_append(&ipc->buffer, nread);

  /* It is just easier to fail here, and not much point in handling it */
  ASSERT(r >= 0, "Unexpected allocation failure in IPC ring buffer");

  bud_ipc_parse(ipc);

  /* Accept handles */
  while (uv_pipe_pending_count(ipc->handle) > 0) {
    uv_handle_type pending;

    pending = uv_pipe_pending_type(ipc->handle);
    if (pending == UV_UNKNOWN_HANDLE)
      continue;

    ASSERT(pending == UV_TCP, "received non-tcp handle on ipc");
    bud_clog(ipc->config, kBudLogDebug, "received handle on ipc");

    ASSERT(ipc->client_cb != NULL, "ipc client_cb not initialized");
    bread_crumb();
    ipc->client_cb(ipc);
  }
}


void bud_ipc_parse(bud_ipc_t* ipc) {
  /* Loop while there is some data to parse */

  int max_int_width = numeric_width_base10(INT_MAX);

  while (ringbuffer_size(&ipc->buffer) >= ipc->waiting) {
    bread_crumb_str("Got a state: %d\n", ipc->state);
    switch (ipc->state) {
      case kBudIPCType:
        {
          uint8_t type;
          size_t len;

          len = 1;
          type = *(uint8_t*) ringbuffer_read_next(&ipc->buffer, &len);
          ASSERT(len >= 1, "Expected at least one byte");

          ringbuffer_read_skip(&ipc->buffer, 1);

          /* Consume Balance byte */
          if (type == kBudIPCBalance) {
            continue;
          } else if (type == kBudIPCConfigFile) {
            bread_crumb_str("Config file received");
          }

          /* Wait for full header */
          ipc->waiting = BUD_IPC_HEADER_SIZE;
          ipc->state = kBudIPCHeader;
        }
        break;
      case kBudIPCHeader:
        {
          size_t len;
          char* header;

          len = max_int_width;
          header = ringbuffer_read_next(&ipc->buffer, &len);
          ASSERT(len >= 1, "Expected at least one digit");
          bread_crumb_str("Header here: %s\n", header);
          continue;
        }
        break;
      case kBudIPCBody:
        {
          size_t len;
          char* tmp;
          long config_len;
          bud_error_t err;
          bud_config_t *config;

          len = max_int_width;
          tmp = ringbuffer_read_next(&ipc->buffer, &len);
          ASSERT(len >= 1, "Expected at least one digit");
          bread_crumb_str("Header here: %s\n", tmp);

          config_len = atol(tmp);
          ringbuffer_read_skip(&ipc->buffer, len);

          tmp = malloc(config_len); 
          if (tmp == NULL) {
            goto failed_alloc;
          }

          config = bud_config_load(tmp, 1, &err);
          ipc->config = config;

          free(tmp);
        }
        break;
    }
  }

failed_alloc:
 bud_clog(ipc->config, kBudLogDebug, "received handle on ipc");
}


bud_error_t bud_ipc_send_config(
                bud_ipc_t* ipc, char* config, const size_t config_len) {

  int r;
  int len_width;
  char* len_as_str;

  uv_buf_t buf;
  bud_error_t err;
  bud_ipc_msg_handle_t* handle;

  /* Allocate space for a IPC write request */
  handle = malloc(sizeof(*handle));
  if (handle == NULL) {
    err = bud_error_str(kBudErrNoMem, "bud_ipc_msg_handle_t");
    goto failed_malloc;
  }

  handle->ipc = ipc;

  r = uv_tcp_init(ipc->config->loop, &handle->tcp);
  if (r != 0) {
    err = bud_error(kBudErrIPCConfigInit);
    goto failed_tcp_init;
  }

  /* Send the configuration string length first */

  len_as_str = padded_int_str(config_len, &len_width);
  if (len_as_str == NULL) {
    err = bud_error_str(kBudErrNoMem, "config_send");
    goto failed_malloc;
  }

  buf = uv_buf_init(len_as_str, len_width);
  r = uv_write2(&handle->req,
                (uv_stream_t*) ipc->handle,
                &buf,
                1,
                (uv_stream_t*) &handle->tcp,
                bud_ipc_msg_send_cb);

  free(len_as_str);

  if (r != 0) {
    err = bud_error_num(kBudErrIPCConfigWrite, r);
    goto failed_accept;
  }

  /* Now we can send the config string */
  buf = uv_buf_init(config, config_len);
  bread_crumb_str("Buf: %p config: %s config_len: %zd\n", &buf, config, config_len);

  r = uv_write2(&handle->req,
                (uv_stream_t*) ipc->handle,
                &buf,
                1,
                (uv_stream_t*) &handle->tcp,
                bud_ipc_msg_send_cb);

  if (r != 0) {
    err = bud_error_num(kBudErrIPCConfigWrite, r);
    goto failed_accept;
  }

  err = bud_ipc_start(ipc);
  if (!bud_is_ok(err))
    goto failed_ipc_start;
  bread_crumb_str("Accepted!!");
  return bud_ok();

failed_accept:
  bread_crumb_str("Failed to accept: r: %s", strerror(r));
  uv_close((uv_handle_t*) &handle->tcp, bud_ipc_msg_handle_on_close);
  return err;

failed_ipc_start:
  bread_crumb_str("Failed ipc start: r:: %s", strerror(r));
  uv_close((uv_handle_t*) &handle->tcp, bud_ipc_msg_handle_on_close);
  return err;

failed_tcp_init:
  bread_crumb_str("Failed to init tcp r:: %d", r);
  free(handle);

failed_malloc:
  bread_crumb_str("Failed malloc");
  return err;
}


void bud_header_to_error_codes(
              bud_ipc_type_t header, bud_error_code_t *i_err_code,
              bud_error_code_t *w_err_code, bud_error_code_t *a_err_code) {
  switch (header) {
    case kBudIPCBalance:
      {
        *i_err_code = kBudErrIPCBalanceInit;
        *w_err_code = kBudErrIPCBalanceWrite;
        *a_err_code = kBudErrIPCBalanceAccept;
      }
      break;
    default:
      {
        *i_err_code = kBudErrIPCConfigInit;
        *w_err_code = kBudErrIPCConfigWrite;
        *a_err_code = kBudErrIPCConfigAccept;
      }
  }
}


bud_error_t bud_ipc_header_handler(
                      bud_ipc_t* ipc,
                      uv_stream_t* server, bud_ipc_type_t header) {

  bud_error_code_t i_err_code;
  bud_error_code_t w_err_code;
  bud_error_code_t a_err_code;

  bud_error_t err;
  int r;
  uint8_t type;
  uv_buf_t buf;
  bud_ipc_msg_handle_t* handle;

  bud_header_to_error_codes(header, &i_err_code, &w_err_code, &a_err_code);

  bread_crumb_str("Sending the header first\n");
  /* Allocate space for a IPC write request */
  handle = malloc(sizeof(*handle));
  if (handle == NULL) {
    bread_crumb_str("Failed to allocate space for the write request!");
    err = bud_error_str(kBudErrNoMem, "bud_ipc_msg_handle_t");
    goto failed_malloc;
  }

  handle->ipc = ipc;

  r = uv_tcp_init(ipc->config->loop, &handle->tcp);
  if (r != 0) {
    bread_crumb_str("Failed to initialize the tcp connection");
    err = bud_error(i_err_code);
    goto failed_tcp_init;
  }

  /* Accept handle */
  r = uv_accept(server, (uv_stream_t*) &handle->tcp);
  bread_crumb_str("Onto accepting the handle. r: %d\n", r);
  if (r != 0) {
    err = bud_error(a_err_code);
    goto failed_accept;
  }

  bread_crumb_str("r: %d\n", r);

  /* Init IPC message */
  type = header;
  buf = uv_buf_init((char *)&type, sizeof(type));

  r = uv_write2(&handle->req,
                (uv_stream_t*) ipc->handle,
                &buf,
                1,
                (uv_stream_t*) &handle->tcp,
                bud_ipc_msg_send_cb);

  if (r != 0) {
    err = bud_error_num(w_err_code, r);
    goto failed_accept;
  }

  return bud_ok();

failed_accept:
  uv_close((uv_handle_t*) &handle->tcp, bud_ipc_msg_handle_on_close);
  return err;

failed_tcp_init:
  free(handle);

failed_malloc:
  return err;
}


bud_error_t bud_ipc_balance(bud_ipc_t* ipc, uv_stream_t* server) {
  return bud_ipc_header_handler(ipc, server, kBudIPCBalance);
}


bud_error_t bud_ipc_config_header_send(bud_ipc_t* ipc, uv_stream_t* server) {
  return bud_ipc_header_handler(ipc, server, kBudIPCConfig);
}


void bud_ipc_msg_handle_on_close(uv_handle_t* handle) {
  bud_ipc_msg_handle_t* msg;

  msg = container_of(handle, bud_ipc_msg_handle_t, tcp);
  free(msg);
}


void bud_ipc_msg_send_cb(uv_write_t* req, int status) {
  bud_ipc_msg_handle_t* msg;
 
  bread_crumb_str("Status: %d\n", status);

  msg = container_of(req, bud_ipc_msg_handle_t, req);
  bread_crumb_str("msg_handle: %p", msg);
  uv_close((uv_handle_t*) &msg->tcp, bud_ipc_msg_handle_on_close);

  /* Ignore ECANCELED */
  if (status == UV_ECANCELED)
    return;

  /* Error */
  if (status != 0) {
    /* XXX Probably report to caller? */
    bud_clog(msg->ipc->config,
             kBudLogWarning,
             "ipc send_cb() failed with (%d) \"%s\"",
             status,
             uv_strerror(status));
  }
}


uv_stream_t* bud_ipc_get_stream(bud_ipc_t* ipc) {
  ASSERT(ipc->handle != NULL, "IPC get stream before init");
  return (uv_stream_t*) ipc->handle;
}
