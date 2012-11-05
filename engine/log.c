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

struct log *log_new(const char *path)
{
	int max;
	struct log *log = xcalloc(1, sizeof(struct log));

	memcpy(log->path, path, strlen(path));
	log->buf = buffer_new(1024 * 1024 *16); /* 10MB buffer*/
	max = _find_maxno_log(log);

	log->no = max + 1;

	if (max > 0) {
		//todo recovery	
	} 

	_make_log_name(log, log->no);
	log->fd = n_open(log->file, N_CREAT_FLAGS, 0644);
	if (log->fd == -1)
		__PANIC("create log file %s....", log->file);

	return log;
}

void log_append(struct log *log, struct slice *sk, struct slice *sv)
{
	int res;
	int len;
	char *block;

	buffer_putint(log->buf, sk->len);
	buffer_putnstr(log->buf, sk->data, sk->len);

	if (sv) {
		buffer_putshort(log->buf, 1);
		buffer_putint(log->buf, sv->len);
		buffer_putnstr(log->buf, sv->data, sv->len);
	}

	len = log->buf->NUL;
	block = buffer_detach(log->buf);

	res = write(log->fd, block, len);
	if (res == -1) {
		__ERROR("log error!!!");
	}
}

void log_free(struct log *log)
{
	buffer_free(log->buf);
	free(log);
}
