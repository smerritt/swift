// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package containerstore

import "sync"

// The two useful functions in here are lockContainerForRead() and lockContainerForWrite().

type innerLock struct {
	lock     sync.RWMutex
	refCount uint
}

type lockManager struct {
	outerLock  sync.Mutex
	innerLocks map[string]innerLock
}

func newLockManager() lockManager {
	return lockManager{innerLocks: make(map[string]innerLock)}
}

// Find the lock for this container. If it exists, we reuse it; if not, we create one.
func (lm *lockManager) getInnerLock(containerHash string) innerLock {
	lm.outerLock.Lock()
	defer lm.outerLock.Unlock()
	inner, ok := lm.innerLocks[containerHash]
	if ok {
		inner.refCount++
	} else {
		inner = innerLock{refCount: 1}
		lm.innerLocks[containerHash] = inner
	}
	return inner
}

func (lm *lockManager) forgetInnerLock(containerHash string) {
	lm.outerLock.Lock()
	defer lm.outerLock.Unlock()

	inner := lm.innerLocks[containerHash]
	inner.refCount--
	if inner.refCount == 0 {
		delete(lm.innerLocks, containerHash)
	}
}

func (lm *lockManager) lockContainerForRead(containerHash string) func() {
	inner := lm.getInnerLock(containerHash)
	inner.lock.RLock()

	// Return an unlocker function. When called, it will release the container lock and decrement the refcount, deleting
	// the container lock if the refcount falls to zero.
	return func() {
		inner.lock.RUnlock()
		lm.forgetInnerLock(containerHash)
	}
}

func (lm *lockManager) lockContainerForWrite(containerHash string) func() {
	inner := lm.getInnerLock(containerHash)
	inner.lock.Lock()

	// Return an unlocker function. When called, it will release the container lock and decrement the refcount, deleting
	// the container lock if the refcount falls to zero.
	return func() {
		inner.lock.Unlock()
		lm.forgetInnerLock(containerHash)
	}
}
