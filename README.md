<div align="center" markdown>

<img src="https://github.com/user-attachments/assets/09252852-3f49-4bad-b665-14e52458e4c0" style="width: 70%;"/>

# Data Commander

<p align="center">
  <a href="#Overview">Overview</a> •
  <a href="#Key-Features">Key Features</a> •
  <a href="#How-to-Use">How to Use</a>
</p>

[![](https://img.shields.io/badge/supervisely-ecosystem-brightgreen)](https://ecosystem.supervisely.com/apps/supervisely-ecosystem/data-commander)
[![](https://img.shields.io/badge/slack-chat-green.svg?logo=slack)](https://supervisely.com/slack)
![GitHub release (latest SemVer)](https://img.shields.io/github/v/release/supervisely-ecosystem/data-commander)

</div>

# Overview

### Data Commander: Your Data Management Solution

Data Commander is a user-friendly tool designed to simplify data organization. It helps you efficiently manage your projects, datasets, and items through an intuitive interface, allowing for easy movement, copying, and processing of data.

### Data Commander as a Service: Streamlined Data Management and Transfer

Data Commander as a Service offers two main functionalities:

1. **Move/Copy:**
   Effortlessly move or copy projects, datasets, and items across different hierarchy levels, while preserving the structure and annotations. Destination project definitions are updated automatically.

2. **Transfer:**
   All annotated entities from `completed` Labeling Jobs with an `accepted` status are included in the transfer. During the process, entities are **copied** to the destination, and a new backup version of the destination project is created. The hierarchy is maintained where possible, making it easy to manage your data across various structures. The destination project definitions are also updated to reflect the changes.

# Key Features

### As a Tool - **Data Commander**:

-   A separate interface in the style of a commander.
-   Additional action settings that expand data interaction capabilities.
-   Hotkeys similar to Norton or other similar commanders.
-   A dedicated taskbar - everything is here and now.
-   Data structure is preserved.

### As a Service - **Move/Copy**:

-   Familiar Supervisely interface.
-   Works with data: projects, datasets, and items.
-   Simple copy and move actions without lengthy decision-making:
    -   Data and their annotations move together.
    -   If entities have matching names, they are not moved to avoid conflicts. For example, if an image with the same name already exists, neither it nor its annotation in the source project will overwrite the entity in the destination project.
-   Data structure is preserved.

### As a Service - **Transfer Annotated Items**:

-   Familiar Supervisely interface.
-   Copies items and their annotations from one location to another while maintaining structure.
-   Copying can be initiated from: projects, datasets, labeling jobs*, and queues*.
-   Copies only items that meet the following conditions:
    -   Were annotated within a Labeling Job, and these jobs are marked **as completed**.
    -   Were marked **as accepted** during review.
    -   Have no remaining labeling jobs with this items in an unfinished status.
-   Automatically resolves conflicts by renaming conflicting objects with a sequential index.
-   Automatically creates a backup version for the destination project, allowing you to create a copy of the project at any time with the same state it had after the transfer and run experiments on the exact same data. `Сurrently only for images`

\* - coming soon

**Hierarchy of Transfers and Result:**

1. When initiating the process from the following instances, the system searches for related Labeling Jobs and exports items based on their results:

    - **Project → Workspace** → A new project is created, preserving the structure.
    - **Project → Project** → A dataset with the source project's name is created in the destination project, preserving nesting.
    - **Project → Dataset** → Similar to the previous point, but the dataset is nested inside the destination dataset.
    - **Dataset → Project** → A top-level dataset with the same name is created, containing only the items from that dataset (excluding nested datasets). You can also choose a few nested datasets - they will be transferred on the same destination level, creating a flat structure.
    - **Dataset → Dataset** → Similar to the previous point, but the dataset is nested inside the destination dataset.

2. `Coming soon` When initiating the process from the following instances, processing is limited to the specified Labeling Jobs, and items are exported based on their results:
    - **Job** follows the same logic as **Dataset**. This means that if some items in an unrelated job have already been annotated and are in the required status, they will not be included.
    - **Queue** follows the same logic as **Job**, but considers all its jobs as the only recognized completed ones.

# How to Use

### As a tool `Data Commander`:

**1.** Go to the `Data Commander`

 <p align="center">
    <img width="80%" src="https://github.com/user-attachments/assets/aae1e64c-50b2-4baa-b17b-b81861bfd9bf" />
 </p>

**2.** Navigate through the structures and select items

 <p align="center">
 <img width="80%" src="https://github.com/user-attachments/assets/367fced3-6a53-41e7-bd16-259566f64c80" />
 </p>

**3.** Click on the buttons to perform actions and select additional settings

 <p align="center">
 <img width="80%" src="https://github.com/user-attachments/assets/e57153d2-16c4-4ab4-8584-76598cb3c918" />
 </p>

**4.** Track the task progress in the progress bar

 <p align="center">
 <img width="80%" src="https://github.com/user-attachments/assets/a28bb909-12fa-4f95-a817-78d852671456" />
 </p>

### As a service **Move/Copy:**

**1.** In the `Projects` management section, select the projects, datasets or items and tick the required checkboxes.

<p align="center">
<img width="80%" src="https://github.com/user-attachments/assets/bdb91202-6d15-404e-9896-556608fc2f32" />
</p>

also if you are managing one project or dataset, it is possible to open the **Organize** menu from the context menu

<p align="center">
<img width="80%" src="https://github.com/user-attachments/assets/87ced130-3916-486c-a3a6-9ecf656294cd" />
</p>

**2.** A new button will appear in the header: `With N selected`.

**3.** Choose the `Move selected` action or `Copy selected`

**4.** A modal window will appear, allowing you to select the destination for the items. <br>
You can create a new project or dataset during this process. <br>
Info tip will help you to understand the process. Select the desired destination and click the `Move` or `Copy` button.

<p align="center">
<img width="80%" src="https://github.com/user-attachments/assets/21b4e127-b7d6-4aa7-95fe-ebb19eaab7f4" />
</p>

**5.** You will be redirected to the **Tasks** page to track the task progress.

<p align="center">
<img width="80%" src="https://github.com/user-attachments/assets/7f5c1667-1622-482a-9fe4-52c738d749b4" />
</p>

### As a service **Transfer Annotated Items:**

**1.** In the `Projects` management section, select the projects or datasets and tick the required checkboxes.

<p align="center">
<img width="80%" src="https://github.com/user-attachments/assets/ac10e0d3-3595-4a7e-a375-822e8ec0d65c" />
</p>

or if you are managing one project or dataset, it is possible to open the **Organize** menu from the context menu

<p align="center">
<img width="80%" src="https://github.com/user-attachments/assets/87ced130-3916-486c-a3a6-9ecf656294cd" />
</p>

**2.** A new button will appear in the header: `With N selected`.

**3.** Choose the `Transfer annotated items` action.

**4.** A modal window will appear, allowing you to select the destination for the items. <br>
You can create a new project or dataset during this process. <br>
Select the desired destination and click the `Transfer` button.

<p align="center">
<img width="80%" src="https://github.com/user-attachments/assets/1d586153-a2a6-426a-adad-8774c2830a34" />
</p>

The image shows that it is not possible to move annotated items to the workspace level, and the `Transfer` button is disabled. To see the available options, check the tooltip. For more details, follow the link from the tooltip.

**5.** Once you select the correct directory and click the **Transfer** button, you will be redirected to the **Tasks** page to track the task progress.

**Transferring Process Explained**

Once the processing of datasets selected in **Step 1** begins, the system will search for related **Labeling Jobs**, and items marked as `accepted` only in `completed` jobs will be transferred.

In our case, we have job `#1`, which is completed, and **30** items have been marked as `accepted`. However, there is also a second job `#2` for the same dataset, which is still in the `on review` stage. Since both jobs contain the same set of items, no items will be transferred for this dataset until job `#2` is `completed`, even though items in job `#1` are already `accepted`.

<p align="center">
<img width="80%" src="https://github.com/user-attachments/assets/23019ef3-83d7-4bcf-a75e-f0c553873ddc" />
</p>

Once job `#2` is `completed` and these items are no longer associated with any other job, a repeat transferring operation will export all items marked as `accepted` across all `completed` jobs for this dataset, even if some of items were marked as `rejected` in other completed jobs.

<p align="center">
<img width="80%" src="https://github.com/user-attachments/assets/86f6a67b-119b-45b7-a562-f0df4c6b697b" />
</p>

As a result, we will obtain 40 annotated items. This is because out of the 13 accepted items from job `#2`, 10 were unique, and 3 were already accepted in job `#1`.

<p align="center">
<img width="80%" src="https://github.com/user-attachments/assets/b36ae58f-a09a-4051-9e39-a5ad7aa88e00" />
</p>

Additionally, since the example project is an image-based project, a backup version is automatically created when new images are transferred into it.

<p align="center">
<img width="80%" src="https://github.com/user-attachments/assets/403af482-7934-4e48-bef6-b52c36ab4392" />
</p>
