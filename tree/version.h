/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#ifndef nessDB_VERSION_H_
#define nessDB_VERSION_H_

/*
 * buffered tree layout version
 */
enum layout_version_e {
	LAYOUT_VERSION_3 = 3, /* current layout version */
	LAYOUT_VERSION = LAYOUT_VERSION_3,
	LAYOUT_MIN_SUPPORTED_VERSION = LAYOUT_VERSION_3,
};

#endif
