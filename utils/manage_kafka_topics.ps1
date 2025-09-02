# Requires PowerShell 5.1 or later.
# This script lists or deletes Kafka topics inside a running Docker container.

param(
    # The name of the Docker container running Kafka.
    [Parameter(Mandatory=$true)]
    [string]$KafkaContainerName,

    # The action to perform: 'list' or 'delete'.
    # If not specified, the default action is to list topics.
    [Parameter(Mandatory=$false)]
    [ValidateSet('list', 'delete')]
    [string]$Action = 'list',

    # The name of the topic to delete. Mandatory only if the action is 'delete'.
    [Parameter(Mandatory=$false)]
    [string]$TopicName
)

function List-KafkaTopics {
    param(
        [string]$ContainerName
    )
    Write-Host "Listing Kafka topics in container '$ContainerName'..." -ForegroundColor Green
    
    # The command to list topics inside the Kafka container.
    $command = "docker exec -it $ContainerName kafka-topics --bootstrap-server kafka:9092 --list"
    
    # Execute the command and display the output.
    Invoke-Expression $command
}

function Delete-KafkaTopic {
    param(
        [string]$ContainerName,
        [string]$TopicToDelete
    )
    
    # Check if a topic name was provided.
    if (-not $TopicToDelete) {
        Write-Host "Error: A topic name must be provided to delete a topic." -ForegroundColor Red
        return
    }

    Write-Host "Attempting to delete Kafka topic '$TopicToDelete' in container '$ContainerName'..." -ForegroundColor Green
    
    # The command to delete a topic. Note: This will not work if the topic is not empty.
    $command = "docker exec -it $ContainerName kafka-topics --bootstrap-server kafka:9092 --delete --topic $TopicToDelete"
    
    Write-Host "Executing command: $command"
    
    # Execute the command and display the output.
    Invoke-Expression $command
    Write-Host "Topic deletion request sent. Check Kafka logs for confirmation." -ForegroundColor Yellow
}

# Main script logic
switch ($Action) {
    'list' {
        List-KafkaTopics -ContainerName $KafkaContainerName
    }
    'delete' {
        if (-not $TopicName) {
            Write-Host "Error: The 'delete' action requires a '-TopicName' parameter." -ForegroundColor Red
        } else {
            Delete-KafkaTopic -ContainerName $KafkaContainerName -TopicToDelete $TopicName
        }
    }
    default {
        Write-Host "Invalid action specified. Please use 'list' or 'delete'." -ForegroundColor Red
    }
}

# eof

<# To use this script:
1.  Save the code as a `.ps1` file (e.g., `manage_kafka_topics.ps1`).
2.  Open a PowerShell terminal.
3.  **To list topics:**
    ```powershell
    .\manage_kafka_topics.ps1 -KafkaContainerName "your_kafka_container_name"
    ```
4.  **To delete a topic:**
    ```powershell
    .\manage_kafka_topics.ps1 -KafkaContainerName "your_kafka_container_name" -Action "delete" -TopicName "your_topic_name"
    
Replace `"your_kafka_container_name"` and `"your_topic_name"` with the actual names you are using.
.\utils\manage_kafka_topics.ps1 -KafkaContainerName begin-kafka-1 
.\utils\manage_kafka_topics.ps1 -KafkaContainerName begin-kafka-1 -Action delete -TopicName hr_txn.hr_txn.departments 
.\utils\manage_kafka_topics.ps1 -KafkaContainerName begin-kafka-1 -Action delete -TopicName hr_txn.hr_txn.employee_compensation
#>