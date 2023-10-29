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
case class Action(actionType: Int, fromNode: NodeObject, toNode: NodeObject, fromId: Int, toId: Int, resultingValue: Option[Int], cost: Double) extends NetGraphComponent
