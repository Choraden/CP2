#include "cacti.h"
#include <pthread.h>
#include <stdbool.h>
#include <stdlib.h>
#include <signal.h>

typedef struct queue {
    void **val;
    size_t size;
    size_t on;
    size_t front;
    size_t end;
} queue_t;

queue_t *queue_create() {
    queue_t *queue;
    queue = malloc(sizeof(*queue));
    queue->val = malloc(4 * sizeof(void *));
    queue->size = 4;
    queue->on = 0;
    queue->front = 0;
    queue->end = 0;

    return queue;
}

void queue_destroy(queue_t *q) {
    if (q->val != NULL) {
        free(q->val);
    }
    if (q != NULL) {
        free(q);
    }
}

void realloc_queue(queue_t *q) {
    void *t[q->size];
    for (size_t j = 0, i = q->front; j < q->on; i = ((i + 1) % q->size), j++) {
        t[j] = q->val[i];
    }
    q->val = realloc(q->val, sizeof(void *) * q->size * 2);
    for (size_t j = 0; j < q->on; j++) {
        q->val[j] = t[j];
    }
    q->front = 0;
    q->end = q->size;
    q->size *= 2;
}

void add_on_queue(queue_t *q, void *e) {
    if (q->on >= q->size) {
        realloc_queue(q);
    }
    q->val[q->end] = e;
    q->end = (q->end + 1) % q->size;
    q->on++;
}

void *pop_queue(queue_t *q) {
    size_t poz = q->front;
    q->front = (q->front + 1) % q->size;
    q->on--;

    return q->val[poz];
}

typedef struct actor {
    queue_t *queue;
    role_t *role;
    actor_id_t id;
    bool on_queue;
    bool active;
    void *state;
} actor_t;

actor_t *actor_create(actor_id_t id, role_t *nr) {
    actor_t *act;
    act = malloc(sizeof(*act));
    act->queue = queue_create();
    act->role = nr;
    act->id = id;
    act->on_queue = false;
    act->active = true;
    act->state = NULL;
    return act;
}

void actor_destroy(actor_t *act) {
    queue_destroy(act->queue);
    if (act != NULL) {
        free(act);
    }
}

typedef struct tpool {
    queue_t *queue;
    queue_t *actors;
    bool stop;
    pthread_mutex_t mutex;
    pthread_cond_t work_cond;
    pthread_cond_t working;
    unsigned int actors_number;
    unsigned int dead_actors;
} tpool_t;

tpool_t *tpool_create() {
    tpool_t *new_tp;
    new_tp = malloc(sizeof(*new_tp));
    new_tp->queue = queue_create();
    new_tp->actors = queue_create();
    new_tp->stop = false;
    if (pthread_mutex_init(&new_tp->mutex, 0) != 0) {
        exit(1);
    }
    if (pthread_cond_init(&new_tp->work_cond, 0) != 0) {
        exit(1);
    }
    if (pthread_cond_init(&new_tp->working, 0) != 0) {
        exit(1);
    }
    new_tp->actors_number = 1;
    new_tp->dead_actors = 0;

    return new_tp;
}

void tpool_destroy(tpool_t *t_pool) {
    if (t_pool != NULL) {
        queue_destroy(t_pool->queue);
        for (size_t i = 0; i < t_pool->actors->on; i++) {
            actor_destroy(t_pool->actors->val[i]);
        }
        queue_destroy(t_pool->actors);
        if (pthread_mutex_destroy(&t_pool->mutex) != 0) {
            exit(1);
        }
        if (pthread_cond_destroy(&t_pool->work_cond) != 0) {
            exit(1);
        }
        if (pthread_cond_destroy(&t_pool->working) != 0) {
            exit(1);
        }
        free(t_pool);
    }
}

pthread_t threads[POOL_SIZE + 1];
tpool_t *tp = NULL;
pthread_key_t actor_key;
int dead_threads, interrupted;

int send_message(actor_id_t actor, message_t message) {
    if (pthread_mutex_lock(&tp->mutex) != 0) {
        exit(1);
    }
    if (tp->actors->on <= (size_t) actor) {
        if (pthread_mutex_unlock(&tp->mutex) != 0) {
            exit(1);
        }

        return -2;
    }
    actor_t *act = tp->actors->val[actor];
    if (!act->active) {

        if (pthread_mutex_unlock(&tp->mutex) != 0) {
            exit(1);
        }
        return -1;
    }
    if (tp->stop || act->queue->on >= ACTOR_QUEUE_LIMIT) {
        if (pthread_mutex_unlock(&tp->mutex) != 0) {
            exit(1);
        }
        return -3;
    }
    message_t *msg;
    msg = malloc(sizeof(message_t));
    msg->message_type = message.message_type;
    msg->nbytes = message.nbytes;
    msg->data = message.data;
    add_on_queue(act->queue, msg);
    if (!act->on_queue) {
        act->on_queue = true;
        add_on_queue(tp->queue, act);
        if (pthread_cond_signal(&tp->work_cond) != 0) {
            exit(1);
        }
    }

    if (pthread_mutex_unlock(&tp->mutex) != 0) {
        exit(1);
    }
    return 0;
}


void *thread_work() {
    while (1) {
        if (pthread_mutex_lock(&tp->mutex) != 0) {
            exit(1);
        }

        while (tp->queue->on == 0 && !tp->stop) {
            if (pthread_cond_wait(&tp->work_cond, &tp->mutex) != 0) {
                exit(1);
            }
        }

        if (tp->stop && tp->queue->on == 0) {
            break;
        }

        actor_t *act = pop_queue(tp->queue);
        pthread_setspecific(actor_key, &act->id);
        act->on_queue = false;
        message_t *msg = pop_queue(act->queue);
        if (pthread_mutex_unlock(&tp->mutex) != 0) {
            exit(1);
        }

        if (msg->message_type == MSG_SPAWN) {
            if (pthread_mutex_lock(&tp->mutex) != 0) {
                exit(1);
            }
            if (!tp->stop && tp->actors_number < CAST_LIMIT) {
                actor_id_t new_actor_id = tp->actors_number++;
                actor_t *new_actor = actor_create(new_actor_id, msg->data);
                add_on_queue(tp->actors, new_actor);
                if (pthread_mutex_unlock(&tp->mutex) != 0) {
                    exit(1);
                }

                actor_id_t *actor_id_ptr = &act->id;
                message_t new_msg;
                new_msg.data = actor_id_ptr;
                new_msg.message_type = MSG_HELLO;
                new_msg.nbytes = sizeof(new_msg.data);
                send_message(new_actor_id, new_msg);
            } else {
                if (pthread_mutex_unlock(&tp->mutex) != 0) {
                    exit(1);
                }
            }
        } else if (msg->message_type == MSG_GODIE) {
            if (pthread_mutex_lock(&tp->mutex) != 0) {
                exit(1);
            }
            act->active = false;
            if (pthread_mutex_unlock(&tp->mutex) != 0) {
                exit(1);
            }
        } else if (msg->message_type == MSG_HELLO) {
            act->state = NULL;
            if ((size_t) msg->message_type < act->role->nprompts) {
                act->role->prompts[msg->message_type](&(act->state), sizeof(msg->data), msg->data);
            }
        } else {
            if ((size_t) msg->message_type < act->role->nprompts) {
                act->role->prompts[msg->message_type](&(act->state), sizeof(msg->data), msg->data);
            }
        }
        free(msg);

        if (pthread_mutex_lock(&tp->mutex) != 0) {
            exit(1);
        }

        if (act->queue->on > 0 && !act->on_queue) {
            act->on_queue = true;
            add_on_queue(tp->queue, act);
            if (pthread_cond_signal(&tp->work_cond) != 0) {
                exit(1);
            }
        } else if (!act->active && !act->on_queue) {
            tp->dead_actors++;
            if (tp->dead_actors == tp->actors_number) {
                tp->stop = true;
                if (pthread_cond_broadcast(&tp->work_cond) != 0) {
                    exit(1);
                }
                break;
            }
        }

        if (pthread_mutex_unlock(&tp->mutex) != 0) {
            exit(1);
        }

    }

    if (pthread_cond_signal(&tp->work_cond) != 0) {
        exit(1);
    }

    dead_threads++;

    if (dead_threads == POOL_SIZE) {
        if (interrupted == 0) {
            if (pthread_kill(threads[POOL_SIZE], SIGRTMIN + 1) != 0) {
                exit(1);
            }
        }

        if (pthread_cond_broadcast(&tp->working) != 0) {
            exit(1);
        }
    }


    if (pthread_mutex_unlock(&tp->mutex) != 0) {
        exit(1);
    }

    return NULL;
}

void *thread_sygint() {
    sigset_t block_mask;
    sigemptyset(&block_mask);
    sigaddset(&block_mask, SIGINT);
    sigaddset(&block_mask, SIGRTMIN + 1);
    if (sigprocmask(SIG_BLOCK, &block_mask, 0) != 0) {
        exit(1);
    }

    int sig;
    sigwait(&block_mask, &sig);

    if (sig == SIGINT) {
        if (pthread_mutex_lock(&tp->mutex) != 0) {
            exit(1);
        }
        interrupted = 1;
        tp->stop = true;
        for (size_t i = 0; i < tp->actors_number; i++) {
            actor_t *act = tp->actors->val[i];
            act->active = false;
        }

        if (pthread_mutex_unlock(&tp->mutex) != 0) {
            exit(1);
        }
    }

    return NULL;
}


actor_id_t actor_id_self() {
    actor_id_t *id = pthread_getspecific(actor_key);
    return *id;
}

int actor_system_create(actor_id_t *actor, role_t *const role) {
    if (tp != NULL) {
        return -1;
    }

    dead_threads = 0;
    interrupted = 0;
    tp = tpool_create();
    actor_id_t actor_id = 0;
    actor_t *act = actor_create(actor_id, role);
    *actor = actor_id;
    add_on_queue(tp->actors, act);
    message_t msg;
    msg.data = NULL;
    msg.message_type = MSG_HELLO;
    msg.nbytes = sizeof(msg.data);

    pthread_key_create(&actor_key, NULL);
    if (pthread_create(&threads[POOL_SIZE], NULL, thread_sygint, NULL) != 0) {
        exit(1);
    }

    sigset_t set;
    sigfillset(&set);
    if (pthread_sigmask(SIG_SETMASK, &set, NULL) != 0) {
        exit(1);
    }

    for (unsigned int i = 0; i < POOL_SIZE; i++) {
        if (pthread_create(&threads[i], NULL, thread_work, NULL) != 0) {
            exit(1);
        }

    }

    send_message(actor_id, msg);
    return 0;
}



void actor_system_join(actor_id_t actor) {

    if (pthread_mutex_lock(&tp->mutex) != 0) {
        exit(1);
    }

    if (actor < tp->actors_number) {
        while (dead_threads != POOL_SIZE) {
            if (pthread_cond_wait(&tp->work_cond, &tp->mutex) != 0) {
                exit(1);
            }
        }
    }

    if (pthread_mutex_unlock(&tp->mutex) != 0) {
        exit(1);
    }

    void *retval;
    for (unsigned int i = 0; i <= POOL_SIZE; i++) {
        if(pthread_join(threads[i], &retval) != 0) {
            exit(1);
        }
    }

    pthread_key_delete(actor_key);
    tpool_destroy(tp);
    tp = NULL;
}



