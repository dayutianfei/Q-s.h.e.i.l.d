/**
 * Copyright 2008 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cn.dayutianfei.zookeeper;

/**
 * A component which is connected to a node-cluster / zk-server. But sometimes
 * connections drop. So this is the lifecycle for connections (once they are
 * already connected).
 * 
 */
public interface IConnectedComponent {

  void reconnect();

  void disconnect();

}
