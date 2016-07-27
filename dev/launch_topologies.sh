#!/usr/bin/env bash
LGADAPTERJAR="../examples/target/examples-1.0-SNAPSHOT-jar-with-dependencies.jar"
LGCOREJAR="../core/target/core-1.0-SNAPSHOT-jar-with-dependencies.jar"

#storm jar $LGADAPTERJAR lemongrenade.examples.adapters.AsyncAdapter AsyncAdapter "1a89a013-3516-462b-956d-a431c57f911d" &
#storm jar $LGADAPTERJAR lemongrenade.examples.adapters.CurlAdapter CurlAdapter "71970239-e141-4663-92ca-a7debda25f0e" &
storm jar $LGADAPTERJAR lemongrenade.examples.adapters.HelloWorldAdapter HelloWorldAdapter "5ba206d-51be-48f8-8ef7-0eb9059b9998" &
#storm jar $LGADAPTERJAR lemongrenade.examples.adapters.HelloWorldNodeAdapter HelloWorldNodeAdapter "f067c37a-a708-49c5-982f-5faa4864365d" &
#storm jar $LGADAPTERJAR lemongrenade.examples.adapters.HelloWorldPython3Adapter HelloWorldPython3Adapter "7b4bc436-0029-437a-bdd6-9fae42b17a2f" &
storm jar $LGADAPTERJAR lemongrenade.examples.adapters.HelloWorldPythonAdapter HelloWorldPythonAdapter "e41a9a36-33d1-4022-94a4-e132931e50e1" &
storm jar $LGADAPTERJAR lemongrenade.examples.adapters.PlusBangAdapter PlusBangAdapter "b38e2244-bc51-4b26-866a-e53bf8a9b9a5" &
storm jar $LGCOREJAR lemongrenade.core.coordinator.CoordinatorTopology CoordinatorTopology "00000000-0000-0000-0000-000000000000" &
