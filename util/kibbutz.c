/*
 * Copyright (c) 2012-2015 The nessDB Project Developers. All rights reserved.
 * Code is licensed with BSD.
 *
 */

#include "u.h"

struct todo {
	void (*f)(void *extra);
	void *extra;
	struct todo *next;
	struct todo *prev;
};

struct kid {
	struct kibbutz *k;
};

struct kibbutz {
	int n_workers;
	int pls_shutdown;
	ness_mutex_t mtx;
	ness_cond_t cond;
	pthread_t *workers;
	struct kid *ids;
	struct todo *head, *tail;
};


static void klock(struct kibbutz *k)
{
	ness_mutex_lock(&k->mtx);
}

static void kunlock(struct kibbutz *k)
{
	ness_mutex_unlock(&k->mtx);
}

static void kwait(struct kibbutz *k)
{
	ness_cond_wait(&k->cond, &k->mtx);
}

static void ksingal(struct kibbutz *k)
{
	ness_cond_signal(&k->cond);
}

static void *work_on_kibbutz(void *arg)
{
	struct kid *kid = (struct kid*)arg;
	struct kibbutz *k = kid->k;

	klock(k);
	while (1) {
		while (k->tail) {
			struct todo *item = k->tail;
			k->tail = item->prev;
			if (k->tail == NULL)
				k->head = NULL;
			else {
				ksingal(k);
			}

			kunlock(k);
			item->f(item->extra);
			xfree(item);
			klock(k);
		}

		if (k->pls_shutdown) {
			ksingal(k);
			kunlock(k);
			return NULL;
		}

		kwait(k);
	}
	kunlock(k);
}

struct kibbutz *kibbutz_new(int n_workers)
{
	int i;
	int r;
	struct kibbutz *k;

	k = xcalloc(1, sizeof(*k));
	k->n_workers = n_workers;
	ness_mutex_init(&k->mtx);
	ness_cond_init(&k->cond);
	k->workers = xcalloc(n_workers, sizeof(*k->workers));
	k->ids = xcalloc(n_workers, sizeof(*k->ids));

	for (i = 0; i < n_workers; i++) {
		k->ids[i].k = k;
		r = pthread_create(&k->workers[i], NULL, work_on_kibbutz,
		                   &k->ids[i]);
		if (r != 0) {
			k->n_workers = i;
			break;
		}
	}

	return k;
}

void kibbutz_enq(struct kibbutz *k, void (*f)(void*), void *extra)
{
	struct todo *td = xcalloc(1, sizeof(*td));

	td->f = f;
	td->extra = extra;
	klock(k);
	td->next = k->head;
	td->prev = NULL;
	if (k->head)
		k->head->prev = td;
	k->head = td;
	if (k->tail == NULL)
		k->tail = td;
	ksingal(k);
	kunlock(k);
}

void kibbutz_free(struct kibbutz *k)
{
	klock(k);
	k->pls_shutdown = 1;
	ksingal(k);
	kunlock(k);

	int i;
	for (i = 0; i < k->n_workers; i++) {
		pthread_join(k->workers[i], NULL);
	}
	xfree(k->workers);
	xfree(k->ids);
	ness_cond_destroy(&k->cond);
	xfree(k);
}
