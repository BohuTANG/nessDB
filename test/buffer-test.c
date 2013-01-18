#include "../engine/internal.h"
#include "../engine/buffer.h"
#include "../engine/debug.h"

int main()
{
	struct buffer *buf = buffer_new(1024);

	buffer_putlong(buf, 123456789UL);

	return 1;
}
