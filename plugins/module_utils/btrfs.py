# Copyright (c) 2022, Gregory Furlong <gnfzdz@fzdz.io>
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)
# SPDX-License-Identifier: GPL-3.0-or-later

from __future__ import absolute_import, division, print_function

__metaclass__ = type

from ansible.module_utils.common.text.converters import to_bytes  # type: ignore
from ansible.module_utils.basic import AnsibleModule  # type: ignore
import re
import os


def normalize_subvolume_path(path):
    # type: (str) -> str
    """
    Normalizes btrfs subvolume paths to ensure exactly one leading slash, no trailing slashes and no consecutive slashes.
    In addition, if the path is prefixed with a leading <FS_TREE>, this value is removed.
    """
    fstree_stripped = re.sub(r"^<FS_TREE>", "", path)
    result = re.sub(r"/+$", "", re.sub(r"/+", "/", "/" + fstree_stripped))
    return result if len(result) > 0 else "/"


def replace_path_stem(path, stem, replacement):
    # type: (str, str, str) -> str
    """
    Replace the beginning of the path (=stem) with replacement

    Return 'path' if no replacement is done
    """
    return os.path.normpath(re.sub(r"^" + stem, replacement + "/", path))


class BtrfsModuleException(Exception):
    pass


class BtrfsMountpoint(object):
    """
    Class for Btrfs mountpoints

    Created to avoid using untyped dicts. Could be refactored to @dataclass once
    """

    def __init__(
        self,
        mountpoint,  # type: str
        device,  # type: str
        subvolume_id,  # type: int
        subvolume_path,  # type: str
    ):
        # type: (...) -> None
        """ "
        Class for Btrfs mountpoints
        """
        self.__path = mountpoint  # type: str
        self.__device = device  # type: str
        self.__subvolid = subvolume_id  # type: int
        self.__subvol = subvolume_path  # type: str

    @property
    def path(self):
        # type: (...) -> str
        return self.__path

    @property
    def device(self):
        # type: () -> str
        return self.__device

    @property
    def subvolume_id(self):
        # type: (...) -> int
        return self.__subvolid

    @property
    def subvolume_path(self):
        # type: (...) -> str
        return self.__subvol

    def __str__(self):
        # type: () -> str
        return "[ mountpoint: %s, device: %s, subvolid: %d, subvol: %s ]" % (
            self.__path,
            self.__device,
            self.__subvolid,
            self.__subvol,
        )


class BtrfsCommands(object):
    """
    Provides access to a subset of the Btrfs command line
    """

    def __init__(self, module):
        # type: (AnsibleModule) -> None
        self.__module = module  # type: AnsibleModule
        self.__btrfs = self.__module.get_bin_path("btrfs", required=True)  # type: str

    def filesystem_show(self, provider):
        # type: (BtrfsInfoProvider) -> list[BtrfsFilesystem]
        """
        Parses 'btrfs filesystem show -d' output into a list[dict[str, str | int | list[str] | None]]

        The dicts have fields:
            'label': str
            'uuid': str
            'devices': list[str]
            'mountpoints' :list[str]
            'default_subvolid': int | None

        Example dict:

            [{'label': 'DATA_SSD', 'uuid': '05119d7f-7ab4-46af-98e9-6aeaaf4a3243',
              'devices': ['/dev/nvme0n1p5'], 'mountpoints': [], 'subvolumes': [], 'default_subvolid': None},
             {'label': 'ROOT', 'uuid': '2bdcd61d-079f-5f3d-b79e-05b15cc58a46',
              'devices': ['/dev/nvme0n1p4'], 'mountpoints': [], 'subvolumes': [], 'default_subvolid': None},
             {'label': 'DATA', 'uuid': '0e9ad6b0-4bd3-45c4-a4a7-2b0bf6a15c85',
              'devices': ['/dev/mapper/sda', '/dev/mapper/sdb', '/dev/mapper/sdc'], 'mountpoints': [], 'subvolumes': [], 'default_subvolid': None},
             {'label': 'HOME', 'uuid': '41658c57-0fce-4281-a1f5-649207d7d3de',
             'devices': ['/dev/mapper/sde', '/dev/mapper/sdd'], 'mountpoints': [], 'subvolumes': [], 'default_subvolid': None}]
        """
        command = "%s filesystem show -d" % (self.__btrfs)  # type: str
        (rc, out, err) = self.__module.run_command(command, check_rc=True)  # type: tuple[int, str, str]
        stdout = [x.strip() for x in out.splitlines()]  # type: list[str]
        filesystems = []  # type: list[BtrfsFilesystem]
        current = None  # type: BtrfsFilesystem | None
        for line in stdout:
            if line.startswith("Label"):
                current = self.__parse_filesystem(line, provider)
                filesystems.append(current)
            elif line.startswith("devid") and current is not None:
                current.devices.append(self.__parse_filesystem_device(line))
        return filesystems

    def __parse_filesystem(
        self,
        line,  # type: str
        provider,  # type: BtrfsInfoProvider
    ):
        # type: (...) -> BtrfsFilesystem
        label = re.sub(r"\s*uuid:.*$", "", re.sub(r"^Label:\s*", "", line))  # type: str
        id = re.sub(r"^.*uuid:\s*", "", line)  # type: str

        return BtrfsFilesystem(
            provider=provider,
            uuid=id,
            label=label.strip("'") if label != "none" else None,
        )

    def __parse_filesystem_device(self, line):
        # type: (str) -> str
        return re.sub(r"^.*path\s", "", line)

    def subvolumes_list(self, filesystem):
        # type: (BtrfsFilesystem) -> list[BtrfsSubvolume]
        """
        Returns a list of subvolumes as dicts with shape of {'id': int, 'parent': int | None, 'path': str }

        The "path" is subvolume path internal to the Btrfs filesystem, not VFS filesystem path
        """
        mountpoint = filesystem.mountpoint  # type: BtrfsMountpoint | None
        if mountpoint is None:
            raise BtrfsModuleException(
                "Cannot list subvolumes for unmounted Btrfs filesystem: UUID=%s"
                % filesystem.uuid
            )
        command = "%s subvolume list -tap %s" % (
            self.__btrfs,
            mountpoint.path,
        )
        (rc, out, err) = self.__module.run_command(command, check_rc=True)  # type: tuple[int, str, str]
        if rc != 0:
            raise BtrfsModuleException(
                "'btrfs sbuvolume list' has non-zero exit value %i: %s" % (rc, str(err))
            )
        subvolumes = [BtrfsSubvolume.mk_root_subvolume(filesystem)]  # type: list[BtrfsSubvolume]
        lines = out.splitlines()  # type: list[str]
        if len(lines) > 2:
            subvolumes.extend(
                [
                    self.__parse_subvolume_list_record(filesystem, line)
                    for line in lines[2:]
                ]
            )

        return subvolumes

    def __parse_subvolume_list_record(self, filesystem, line):
        # type: (BtrfsFilesystem, str) -> BtrfsSubvolume
        item = line.split("\t")  # type: list[str]
        return BtrfsSubvolume(
            filesystem=filesystem,
            subvol_id=int(item[0]),
            subvol_path=normalize_subvolume_path(item[5]),
            parent=int(item[2]),
        )

    def subvolume_get_default(self, filesystem_path):
        # type: (str) -> int
        command = [self.__btrfs, "subvolume", "get-default", to_bytes(filesystem_path)]
        (rc, out, err) = self.__module.run_command(command, check_rc=True)  # type: tuple[int, str, str]
        # ID [n] ...
        return int(out.strip().split()[1])

    def subvolume_set_default(self, filesystem_path, subvolume_id):
        # type: (str, int) -> None
        command = [
            self.__btrfs,
            "subvolume",
            "set-default",
            str(subvolume_id),
            to_bytes(filesystem_path),
        ]
        (rc, out, err) = self.__module.run_command(command, check_rc=True)
        if rc != 0:
            raise BtrfsModuleException(
                "couldn't set default subvolume ID (%d) for filesystem: %s : %s"
                % (subvolume_id, filesystem_path, err)
            )

    def subvolume_create(self, filesystem_path):
        # type: (str) -> None
        command = [self.__btrfs, "subvolume", "create", to_bytes(filesystem_path)]
        (rc, out, err) = self.__module.run_command(command, check_rc=True)
        if rc != 0:
            raise BtrfsModuleException(
                "couldn't create subvolume: %s: %s" % (filesystem_path, err)
            )

    def subvolume_snapshot(self, snapshot_source, snapshot_destination):
        # type: (str, str) -> None
        command = [
            self.__btrfs,
            "subvolume",
            "snapshot",
            to_bytes(snapshot_source),
            to_bytes(snapshot_destination),
        ]
        (rc, out, err) = self.__module.run_command(command, check_rc=True)
        if rc != 0:
            raise BtrfsModuleException(
                "couldn't create snapshot: %s from %s: %s"
                % (snapshot_destination, snapshot_source, err)
            )

    def subvolume_delete(self, filesystem_path):
        # type: (str) -> None
        command = [self.__btrfs, "subvolume", "delete", to_bytes(filesystem_path)]
        (rc, out, err) = self.__module.run_command(command, check_rc=True)
        if rc != 0:
            raise BtrfsModuleException(
                "couldn't delete subvolume: %s: %s" % (filesystem_path, err)
            )


class BtrfsInfoProvider(object):
    """
    Utility providing details of the currently available btrfs filesystems
    """

    def __init__(self, module):
        # type: (AnsibleModule) -> None
        self.__module = module  # type: AnsibleModule
        self.__btrfs_api = BtrfsCommands(module)  # type: BtrfsCommands
        self.__findmnt_path = self.__module.get_bin_path("findmnt", required=True)  # type: str

    def get_filesystems(self):
        # type: () -> list[BtrfsFilesystem]
        """
        Returns list[dict] of filesystems. Each filesystem is described by a dict with fields:

        "devices": list[str]
        "mountpoints": list[str]
        "subvolumes":
        "default_subvolid": int

        """
        filesystems = self.__btrfs_api.filesystem_show(self)  # type: list[BtrfsFilesystem]
        mountpoints = self.__find_mountpoints()  # type: list[BtrfsMountpoint]
        for filesystem in filesystems:
            device_mountpoints = self.__filter_mountpoints_for_devices(
                mountpoints,
                filesystem.devices,
            )
            filesystem.update_mountpoints(device_mountpoints)  # type: ignore

            if len(device_mountpoints) > 0:
                # any path within the filesystem can be used to query metadata
                mountpoint = device_mountpoints[0]  # type: BtrfsMountpoint
                filesystem.update_subvolumes(self.get_subvolumes(filesystem))
                filesystem.update_default_subvolume_id(
                    self.get_default_subvolume_id(mountpoint.path)
                )
        # raise BtrfsModuleException(f"filesystems: {filesystems}")
        return filesystems

    def get_mountpoints(self, filesystem_devices):
        # type: (list[str]) -> list[BtrfsMountpoint]
        mountpoints = self.__find_mountpoints()
        return self.__filter_mountpoints_for_devices(mountpoints, filesystem_devices)

    def get_subvolumes(self, filesystem):
        # type: (BtrfsFilesystem) -> list[BtrfsSubvolume]
        """
        Returns a list of subvolumes
        """
        return self.__btrfs_api.subvolumes_list(filesystem)

    def get_default_subvolume_id(self, filesystem_path):
        # type: (str) -> int
        """
        Returns "default" subvolume id. That is often 5. It does not mean that the default subvolume is mounted.
        """
        return self.__btrfs_api.subvolume_get_default(filesystem_path)

    def __filter_mountpoints_for_devices(self, mountpoints, devices):
        # type: (list[BtrfsMountpoint], list[str]) -> list[BtrfsMountpoint]
        return [mp for mp in mountpoints if (mp.device in devices)]

    def __find_mountpoints(self):
        # type: () -> list[BtrfsMountpoint]
        """ "
        Parses Btrfs mountpoints from 'findmnt -t btrfs -nvP' output

        Returns: list[BtrfsMountpoint]
        """
        command = "%s -t btrfs -nvP" % self.__findmnt_path  # type: str
        (rc, out, err) = self.__module.run_command(command)  # type: tuple[int, str, str]
        mountpoints = []  # type: list[BtrfsMountpoint]
        if rc == 0:
            for line in out.splitlines():
                mountpoint = self.__parse_mountpoint_pairs(line)  # type: BtrfsMountpoint
                mountpoints.append(mountpoint)
        else:
            raise BtrfsModuleException(
                "findmnt has non-zero exit value %i: %s" % (rc, str(err))
            )
        return mountpoints

    def __parse_mountpoint_pairs(self, line):
        # type: (str) -> BtrfsMountpoint
        """
        Parses mountpoint info from 'findmnt -t btrfs -nvP' output

        Returns BtrfsMountpoint
        """
        pattern = re.compile(
            r'^TARGET="(?P<target>.*)"\s+SOURCE="(?P<source>.*)"\s+FSTYPE="(?P<fstype>.*)"\s+OPTIONS="(?P<options>.*)"\s*$'
        )
        match = pattern.search(line)  # type: re.Match | None
        if match is not None:
            groups = match.groupdict()
            mount_options = self.__extract_mount_options(groups["options"])  # type: dict[str, str | None]

            try:
                subvolid = mount_options["subvolid"]  # type: str | None
                subvol = mount_options["subvol"]  # type: str | None
                if subvol is None or subvolid is None:
                    raise ValueError(
                        "subvolid or subvol is None"
                    )  # this should never happen

                return BtrfsMountpoint(
                    mountpoint=groups["target"],
                    device=groups["source"],
                    subvolume_id=int(subvolid),
                    subvolume_path=subvol,
                )
            except Exception as err:
                raise BtrfsModuleException(
                    "Failed to parse mountpoint from line: %s, Error: %s"
                    % (line, str(err))
                )
        else:
            raise BtrfsModuleException(
                "Failed to parse findmnt result for line: %s" % line
            )

    # replaced with __extract_mount_options()
    # def __extract_mount_subvolid(self, mount_options):
    #     for option in mount_options.split(","):
    #         if option.startswith("subvolid="):
    #             return int(option[len("subvolid=") :])
    #     raise BtrfsModuleException(
    #         "Failed to find subvolid for mountpoint in options '%s'" % mount_options
    #     )

    def __extract_mount_options(self, mount_options):
        # type: (str) -> dict[str, str | None]
        res = dict()  # type: dict[str, str | None]
        for option in mount_options.split(","):
            param = option.split("=", maxsplit=1)
            res[param[0]] = param[1] if len(param) == 2 else None
        return res


class BtrfsSubvolume(object):
    """
    Wrapper class providing convenience methods for inspection of a btrfs subvolume
    """

    def __init__(
        self,
        filesystem,  # type: BtrfsFilesystem
        subvol_id,  # type: int
        subvol_path,  # type:  str
        parent,  # type: int | None
        mountpoints=[],  # type: list[BtrfsMountpoint]
    ):
        # type: (...) -> None
        self._filesystem = filesystem  # type: BtrfsFilesystem
        self._id = subvol_id  # type: int
        self._path = subvol_path  # type: str
        self._parent = parent  # type: int | None
        self._mountpoints = mountpoints  # type: list[BtrfsMountpoint]

    @classmethod
    def mk_root_subvolume(cls, filesystem):
        # type: (BtrfsFilesystem) -> "BtrfsSubvolume"
        return cls(filesystem=filesystem, subvol_id=5, subvol_path="/", parent=None)

    @property
    def filesystem(self):
        # type: () -> BtrfsFilesystem
        return self._filesystem

    @property
    def is_mounted(self):
        # type: () -> bool
        return len(self.mountpoints) > 0

    @property
    def is_filesystem_root(self):
        # type: () -> bool
        return 5 == self._id

    @property
    def is_filesystem_default(self):
        # type: () -> bool
        return self._filesystem.default_subvolid == self._id

    @property
    def __info(self):
        return self._filesystem.get_subvolume_info_for_id(self._id)

    @property
    def id(self):
        return self._id

    @property
    def name(self):
        return self.path.split("/").pop()

    @property
    def path(self):
        # type: () -> str
        return self._path

    @property
    def parent(self):
        # type: () -> int | None
        return self._parent

    @property
    def mountpoints(self):
        # type: () -> list[BtrfsMountpoint]
        return self._mountpoints

    def add_mountpoint(self, mountpoint):
        # type: (BtrfsMountpoint) -> None
        if mountpoint not in self._mountpoints:
            self._mountpoints.append(mountpoint)

    def remove_mountpoint(self, mountpoint):
        # type: (BtrfsMountpoint) -> None
        self._mountpoints = [
            mp for mp in self._mountpoints if mp.subvolume_id != mountpoint.subvolume_id
        ]

    # TODO: complete mountpoints refactor

    def __str__(self) -> str:
        return "id: %d, path: %s, parent: %s" % (
            self._id,
            self._path,
            str(self._parent) if self._parent is not None else "-",
        )

    def get_mounted_path(self):
        # type: () -> BtrfsMountpoint | None
        """
        Return BtrfsMountpoint of the BtrfsSubvolume or its parent subvolume.
        Returns None if not mountpoint found
        """
        mountpoints = self.get_mountpoints()
        if len(mountpoints) > 0:
            return mountpoints[0]
        elif self.parent is not None:
            parent = self._filesystem.get_subvolume_by_id(self.parent)
            return parent.get_mounted_path() if parent is not None else None
        else:
            return None

    def get_mountpoints(self):
        # type: () -> list[BtrfsMountpoint]
        return self._filesystem.get_mountpoints_by_subvolume_id(self._id)

    def get_child_relative_path(self, absolute_child_path):
        """
        Get the relative path from this subvolume to the named child subvolume.
        The provided parameter is expected to be normalized as by normalize_subvolume_path.
        """
        path = self.path
        if absolute_child_path.startswith(path):
            relative = absolute_child_path[len(path) :]
            return re.sub(r"^/*", "", relative)
        else:
            raise BtrfsModuleException(
                "Path '%s' doesn't start with '%s'" % (absolute_child_path, path)
            )

    def get_parent_subvolume(self):
        # type: () -> BtrfsSubvolume | None
        if self._parent is not None:
            return self._filesystem.get_subvolume_by_id(self._parent)
        else:
            return None

    def get_child_subvolumes(self):
        return self._filesystem.get_subvolume_children(self._id)


class BtrfsFilesystem(object):
    """
    Wrapper class providing convenience methods for inspection of a btrfs filesystem
    """

    def __init__(
        self,
        provider,  # type: BtrfsInfoProvider
        uuid,  # type: str
        label=None,  # type: str | None
        default_subvolid=5,  # type: int
        devices=[],  # type: list[str]
        mountpoints=[],  # type: list[BtrfsMountpoint]
        subvolumes=[],  # type: list[BtrfsSubvolume]
    ):
        # type: (...) -> None
        self.__provider = provider

        # constant for module execution
        self.__uuid = uuid  # type: str
        self.__label = label  # type: str | None
        self.__default_subvolid = default_subvolid  # type: int
        self.__devices = devices  # type: list[str]
        self.__mountpoints = dict()  # type: dict[int, list[BtrfsMountpoint]]
        self.__subvolumes = dict()  # type: dict[int, BtrfsSubvolume]

        # refreshable
        self.update_mountpoints(mountpoints)
        self.update_subvolumes(subvolumes)

    @property
    def uuid(self):
        # type: () -> str
        return self.__uuid

    @property
    def label(self):
        # type: () -> str | None
        return self.__label

    @property
    def default_subvolid(self):
        # type: () -> int
        return self.__default_subvolid

    @property
    def devices(self):
        # type: () -> list[str]
        return self.__devices

    @property
    def subvolumes(self):
        # type: () -> dict[int, BtrfsSubvolume]
        return self.__subvolumes

    @property
    def mountpoints(self):
        # type: () -> dict[int, list[BtrfsMountpoint]]
        return self.__mountpoints

    @property
    def mountpoint(self):
        # type: () -> BtrfsMountpoint | None
        """
        Returns a mountpoint that happens to be the first in the self.mountpoints
        or None if no mountpoints found
        """
        try:
            for mountpoints in self.mountpoints.values():
                return mountpoints[0]
        except IndexError:
            pass
        return None

    def set_label(self, label):
        # type: (str) -> None
        self.__label = label

    def __str__(self):
        # type: () -> str
        return "label: %s, UUID: %s, devices: %s, mountpoints: %s, subvolumes: %s" % (
            self.__label,
            self.__uuid,
            str(self.devices),
            str(self.mountpoints),
            str(self.subvolumes),
        )

    def refresh(self):
        # type: () -> None
        self.refresh_mountpoints()
        self.refresh_subvolumes()
        self.refresh_default_subvolume()

    def refresh_mountpoints(self):
        # type: () -> None
        mountpoints = self.__provider.get_mountpoints(self.__devices)
        self.update_mountpoints(mountpoints)

    def update_mountpoints(
        self,
        mountpoints,  # type: list[BtrfsMountpoint]
    ):
        # type: (...) -> None
        self.__mountpoints = dict()
        for mp in mountpoints:
            subvolid = mp.subvolume_id  # type: int
            if subvolid not in self.__mountpoints:
                self.__mountpoints[subvolid] = []
            self.__mountpoints[subvolid].append(mp)

    def refresh_subvolumes(self):
        # type: () -> None
        filesystem_path = self.get_any_mountpoint()
        if filesystem_path is not None:
            subvolumes = self.__provider.get_subvolumes(self)
            self.update_subvolumes(subvolumes)

    def update_subvolumes(self, subvolumes):
        # type: (list[BtrfsSubvolume]) -> None
        # TODO strategy for retaining information on deleted subvolumes?
        self.__subvolumes = dict()
        for subvolume in subvolumes:
            if subvolume.id in self.__mountpoints:
                subvolume._mountpoints = self.__mountpoints[subvolume.id]
            self.__subvolumes[subvolume.id] = subvolume

    def update_default_subvolume_id(self, subvol_id):
        # type: (int) -> None
        self.__default_subvolid = subvol_id

    def refresh_default_subvolume(self):
        # type: () -> None
        mountpoint = self.get_any_mountpoint()
        if mountpoint is not None:
            self.__default_subvolid = self.__provider.get_default_subvolume_id(
                mountpoint.path
            )

    def contains_device(self, device):
        # type: (str) -> bool
        return device in self.__devices

    def contains_subvolume(self, subvolume):
        return self.get_subvolume_by_subvolpath(subvolume) is not None

    def get_subvolume_by_id(self, subvolume_id):
        # type: (int) -> BtrfsSubvolume | None
        return (
            self.__subvolumes[subvolume_id]
            if subvolume_id in self.__subvolumes
            else None
        )

    def get_subvolume_info_for_id(self, subvolume_id):
        return (
            self.__subvolumes[subvolume_id]
            if subvolume_id in self.__subvolumes
            else None
        )

    # FIXME: takes subvolume path (as returned by btrfs subvol list -tap) as argument
    # that is internal Btrfs path, not filesystem path. However, the method is called
    # from BtrfsSubvolumeModule methods assuming the "path" is a filesystem path
    # TODO: review btrfs_subvolume functions calling get_subvolume_by_subvolpath()
    def get_subvolume_by_subvolpath(self, subvol_path):
        # type: (str) -> BtrfsSubvolume | None
        """
        Returns BtrfsSubvolume for a subvolume path as returned by
        'btrfs subvolume list -tap MOUNTPOINT_PATH'
        """
        for subvolume in self.__subvolumes.values():
            if subvolume.path == subvol_path:
                return subvolume
        return None

    # TODO: Work in progress
    def get_subvolume_by_path(self, filesystem_path):
        # type: (str) -> BtrfsSubvolume | None
        """
        Return BtrfsSubvolume for the filesystem path.
        Returns None if no matchin subvolume in the filesystem is found
        """
        res = None  # type: BtrfsSubvolume | None
        longest_match = 0  # type: int
        mountpoint = None  # type: BtrfsMountpoint | None
        for subvol, mountpoints in self.mountpoints.items():
            for mp in mountpoints:
                match_len = len(os.path.commonpath([filesystem_path, mp.path]))  # type: int
                if match_len > longest_match:
                    if match_len == 1 and mp != os.pathsep:
                        continue
                    mountpoint = mp
        if mountpoint is None:
            raise BtrfsModuleException(
                "Could not find matching subvolume for path: %s " % filesystem_path
            )

        # TODO: find mountpoint and then find the subvol that matches the path - mountpoint
        return None

    def get_any_mountpoint(self):
        # type: () -> BtrfsMountpoint | None
        for mountpoints in self.__mountpoints.values():
            if len(mountpoints) > 0:
                return mountpoints[0]
        # maybe error?
        return None

    def get_any_mounted_subvolume(self):
        for subvolid, subvol_mountpoints in self.__mountpoints.items():
            if len(subvol_mountpoints) > 0:
                return self.get_subvolume_by_id(subvolid)
        return None

    # FIXME: no mountpoints returned for ROOT FS
    def get_mountpoints_by_subvolume_id(self, subvolume_id):
        # type: (int) -> list[BtrfsMountpoint]
        # raise BtrfsModuleException(
        #     f"BtrfsFilesystem.get_mountpoints_by_subvolume_id(): label: {self.label}, subvol_id: {subvolume_id}, mountpoints: {self.mountpoints}"
        # )
        return (
            self.__mountpoints[subvolume_id]
            if subvolume_id in self.__mountpoints
            else []
        )

    # FIXME: this command is used to give filesystem path as 'subvolume',
    # but the function assumes the argument is intra filesystem Btrfs path
    # FIX logic
    # - Find the mountpoint (again)
    # - find the subvolume by eliminating the mountpoint's subvolume's path away from the subvol
    #   path that is appended to the mountpoint
    def get_nearest_subvolume(self, subvolume):
        """Return the identified subvolume if existing, else the closest matching parent"""
        subvolumes_by_path = self.__get_subvolumes_by_path()
        while len(subvolume) > 1:
            if subvolume in subvolumes_by_path:
                return BtrfsSubvolume(self, subvolumes_by_path[subvolume]["id"])
            else:
                subvolume = re.sub(r"/[^/]+$", "", subvolume)

        return BtrfsSubvolume(self, 5)

    def get_mountpath_as_child(self, subvolume_name):
        """Find a path to the target subvolume through a mounted ancestor"""
        nearest = self.get_nearest_subvolume(subvolume_name)
        if nearest.path == subvolume_name:
            nearest = nearest.get_parent_subvolume()
        if nearest is None or nearest.get_mounted_path() is None:
            raise BtrfsModuleException(
                "Failed to find a path '%s' through a mounted parent subvolume"
                % subvolume_name
            )
        else:
            return (
                nearest.get_mounted_path()
                + os.path.sep
                + nearest.get_child_relative_path(subvolume_name)
            )

    def get_subvolume_children(self, subvolume_id):
        return [
            BtrfsSubvolume(self, x["id"])
            for x in self.__subvolumes.values()
            if x["parent"] == subvolume_id
        ]

    def __get_subvolumes_by_path(self):
        result = {}
        for s in self.__subvolumes.values():
            path = s["path"]
            result[path] = s
        return result

    def is_mounted(self):
        return self.__mountpoints is not None and len(self.__mountpoints) > 0

    def get_summary(self):
        subvolumes = []
        sources = self.__subvolumes.values() if self.__subvolumes is not None else []
        for subvolume in sources:
            id = subvolume["id"]
            subvolumes.append(
                {
                    "id": id,
                    "path": subvolume["path"],
                    "parent": subvolume["parent"],
                    "mountpoints": self.get_mountpoints_by_subvolume_id(id),
                }
            )

        return {
            "default_subvolume": self.__default_subvolid,
            "devices": self.__devices,
            "label": self.__label,
            "uuid": self.__uuid,
            "subvolumes": subvolumes,
        }


class BtrfsFilesystemsProvider(object):
    """
    Provides methods to query available btrfs filesystems
    """

    def __init__(self, module):
        # type: (AnsibleModule) -> None
        self.__module = module  # type: AnsibleModule
        self.__provider = BtrfsInfoProvider(module)
        self.__filesystems = dict()  # type: dict[str, BtrfsFilesystem]

    def get_any_matching_filesystem(self, criteria):
        # type: (dict[str, str | None]) -> BtrfsFilesystem
        if criteria["device"] is not None:
            criteria["device"] = os.path.realpath(criteria["device"])

        self.__check_init()
        matching = [
            f
            for f in self.__filesystems.values()
            if self.__filesystem_matches_criteria(f, criteria)
        ]
        if len(matching) == 1:
            return matching[0]
        else:
            raise BtrfsModuleException(
                "Found %d filesystems matching criteria uuid=%s label=%s device=%s"
                % (
                    len(matching),
                    criteria["uuid"],
                    criteria["label"],
                    criteria["device"],
                )
            )

    def __filesystem_matches_criteria(
        self,
        filesystem,  # type: BtrfsFilesystem
        criteria,  # type: dict[str, str | None]
    ):
        # type: (...) -> bool
        """
        Returns True if the filesystem matches criteria. Criterion with None value matches any value.
        """
        return (
            (criteria["uuid"] is None or filesystem.uuid == criteria["uuid"])
            and (criteria["label"] is None or filesystem.label == criteria["label"])
            and (
                criteria["device"] is None
                or filesystem.contains_device(criteria["device"])
            )
        )

    def get_filesystem_for_device(self, device):
        real_device = os.path.realpath(device)
        self.__check_init()
        for fs in self.__filesystems.values():
            if fs.contains_device(real_device):
                return fs
        return None

    def get_filesystems(self):
        # type: () -> list[BtrfsFilesystem]
        self.__check_init()
        return list(self.__filesystems.values())

    def __check_init(self):
        # type: () -> None
        if len(self.__filesystems) == 0:
            for fs in self.__provider.get_filesystems():
                self.__filesystems[fs.uuid] = fs
