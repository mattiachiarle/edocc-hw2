package NetGraphAlgebraDefs

/*
 *
 *  Copyright (c) 2021. Mark Grechanik and Lone Star Consulting, Inc. All rights reserved.
 *   
 *   Unless required by applicable law or agreed to in writing, software distributed under
 *   the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 *   either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 *  
 */

@SerialVersionUID(123L)
case class NodeObject(id: Int, children: Int, props: Int, currentDepth: Int = 1, propValueRange: Int, maxDepth: Int,
                      maxBranchingFactor: Int, maxProperties: Int, storedValue: Double, valuableData: Boolean = false) extends NetGraphComponent
