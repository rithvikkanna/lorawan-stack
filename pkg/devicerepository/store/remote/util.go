// Copyright © 2020 The Things Network Foundation, The Things Industries B.V.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package remote

import "go.thethings.network/lorawan-stack/v3/pkg/ttnpb"

// DutyCycleFromFloat converts a float value (0 < dc < 1) to a ttnpb.AggregatedDutyCycle
// enum value. The enum value is rounded-down to the closest value, which means
// that dc == 0.3 will return ttnpb.DUTY_CYCLE_4 (== 0.25).
func DutyCycleFromFloat(dc float64) ttnpb.AggregatedDutyCycle {
	counts := 0
	for counts = 0; dc < 1 && counts < 15; counts++ {
		dc *= 2
	}
	return ttnpb.AggregatedDutyCycle(counts)
}
