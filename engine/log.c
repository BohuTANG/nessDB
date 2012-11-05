#include "config.h"

#include "log.h"
#include "debug.h"
#include "xmalloc.h"

void _make_log_name(struct log *log, int lsn)
{
	memset(log->file, 0, NESSDB_PATH_SIZE);
	snprintf(log->file, NESSDB_PATH_SIZE, "%s/%06d%s", log->path, lsn, NESSDB_LOG_EXT);
}

int  _find_maxno_log(struct log *log)
{
	int lsn;
	int max = 0;
	DIR *dd;
	struct dirent *de;
	char name[NESSDB_PATH_SIZE];

	dd = opendir(log->path);
	while ((de = readdir(dd))) {
		if (strstr(de->d_name, NESSDB_LOG_EXT)) {
			memset(name, 0, NESSDB_PATH_SIZE);
			memcpy(name, de->d_name, strlen(de->d_name) - 4);
			lsn = atoi(name);
			max = lsn > max ? lsn : max;
		}
	}
	closedir(dd);

	return max;
}

void log_create(struct log *log)
{
	log->no++;
	_make_log_name(log, log->no);
	__DEBUG("create new log#%s", log->file);

	if (log->fd > 0)
		close(log->fd);

	log->fd = n_open(log->file, N_CREAT_FLAGS, 0644);
	if (log->fd == -1)
		__PANIC("create log file %s....", log->file);
}

void log_remove(struct log *log)
{
	int res;

	_make_log_name(log, log->no - 1);
	__DEBUG("remove log#%s", log->file);

	res = remove(log->file);
	if (res == -1)
		__ERROR("remove log %s error", log->file);
}

struct kv_pair *_log_read(struct log *log)
{
	int l = 2;
	int fd;
	struct kv_pair *first = xcalloc(1, sizeof(struct kv_pair));
	struct kv_pair *pre = first;

	_make_log_name(log, log->no - 1);
	fd = n_open(log->file, N_OPEN_FLAGS, 0644);
	while (fd > -1 && (l--) > 0) {
		int sizes = lseek(fd, 0, SEEK_END);
		int rem = sizes;

		lseek(fd, 0, SEEK_SET);

		while (rem) {
			char *k;
			char *v;
			int klen = 0;
			short opt = 0;
			struct kv_pair *cur  = xcalloc(1, sizeof(struct kv_pair));

			if (read(fd, &klen, sizeof klen) != sizeof klen)
				__PANIC("read klen error");
			rem -= sizeof klen;

			k = xcalloc(1, klen + 1);
			if (read(fd, k, klen) != klen)
				__PANIC("error when read key");

			rem -= klen;

			if (read(fd, &opt, sizeof opt) != sizeof opt)
				__PANIC("error when read opt");
			rem -= sizeof opt;

			cur->sk.data = k;
			cur->sk.len = klen;

			if (opt == 1) {
				int vlen = 0;

				if (read(fd, &vlen, sizeof vlen) != sizeof vlen)
					__PANIC("read vlen error");

				rem -= sizeof vlen;
				v = xcalloc(1, vlen + 1);
				if (read(fd, v, vlen) != vlen)
					__PANIC("error when read value");

				rem -= vlen;

				cur->sv.data = v;
				cur->sv.len = vlen;
			}

			pre->nxt = cur;
			pre = cur;
		}

		_make_log_name(log, log->no);
		fd = n_open(log->file, N_OPEN_FLAGS, 0644);
	}

	return first;
}

struct log *log_new(const char *path, int islog)
{
	int max;
	struct log *log = xcalloc(1, sizeof(struct log));

	memcpy(log->path, path, strlen(path));
	log->buf = buffer_new(1024 * 1024 *16); /* 10MB buffer*/

	max = _find_maxno_log(log);
	log->no = max;
	log->islog = islog;

	if (max > 0)
		log->redo = _log_read(log);

	log_create(log);
	return log;
}

void log_append(struct log *log, struct slice *sk, struct slice *sv)
{
	int res;
	int len;
	char *block;

	if (!log->islog)
		return;

	buffer_putint(log->buf, sk->len);
	buffer_putnstr(log->buf, sk->data, sk->len);

	if (sv) {
		buffer_putshort(log->buf, 1);
		buffer_putint(log->buf, sv->len);
		buffer_putnstr(log->buf, sv->data, sv->len);
	} else 
		buffer_putshort(log->buf, 0);

	len = log->buf->NUL;
	block = buffer_detach(log->buf);

	res = write(log->fd, block, len);
	if (res == -1) {
		__ERROR("log error!!!");
	}
}

void log_free(struct log *log)
{
	if (log->fd > 0)
		close(log->fd);

	buffer_free(log->buf);
	free(log);
}
