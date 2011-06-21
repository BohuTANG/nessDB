#ifndef LIBS_SBUF
#define LIBS_SBUF

#include <stdarg.h>

#if __GNUC__ > 2 || (__GNUC__ == 2 && __GNUC_MINOR__ > 4)
    #define GNUC_PRINTF_CHECK(fmt_idx, arg_idx) __attribute__((format (printf, fmt_idx, arg_idx)))
#else
    #define GNUC_PRINTF_CHECK(fmt_idx, arg_idx)
#endif

#define _S(SB) string_raw((SB))

struct strings;

struct strings *string_new(size_t reserve);
void string_free(struct strings *sb);

void string_clear(struct strings *sb);
char *string_detach(struct strings *sb);
void string_move(struct strings *src, struct strings *dest);
void string_dup(struct strings *src, struct strings *dest);

void string_putc(struct strings *sb, const char c);

void string_cat(struct strings *sb, const char *str);
void string_ncat(struct strings *sb, const char *str, size_t len);
void string_bcat(struct strings *sb, const struct strings *buf);
void string_catf(struct strings *sb, const char *fmt, ...) GNUC_PRINTF_CHECK(2,3);

int string_cmp(struct strings *sb, const char *str);
int string_ncmp(struct strings *sb, const char *str, size_t len);
int string_bcmp(struct strings *sb, const struct strings *buf);

const char *string_raw(struct strings *sb);
size_t string_len(struct strings *sb);

#endif
