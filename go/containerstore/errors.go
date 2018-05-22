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

import "fmt"

type ContainerNotFoundError struct {
	Account   string
	Container string
}

func (e *ContainerNotFoundError) Error() string {
	return fmt.Sprintf("Container %s/%s not found", e.Account, e.Container)
}

type ContainerExistsError struct {
	Account   string
	Container string
}

func (e *ContainerExistsError) Error() string {
	return fmt.Sprintf("Container %s/%s exists", e.Account, e.Container)
}
