import { LogOut, Moon, Sun } from 'lucide-react'
import { Avatar, AvatarFallback, AvatarImage } from '@/components/ui/avatar'
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu'
import { useAuth } from '@/features/auth/auth-context'
import { useTheme } from '@/lib/theme'

function initials(name: string): string {
  return name
    .split(/\s+/)
    .filter(Boolean)
    .slice(0, 2)
    .map((part) => part[0]?.toUpperCase())
    .join('')
}

export function UserBlock() {
  const { user, signOut } = useAuth()
  const { theme, toggle } = useTheme()

  if (!user) {
    return null
  }
  return (
    <DropdownMenu>
      <DropdownMenuTrigger className="flex w-full items-center gap-3 rounded-lg border-t border-sidebar-border px-2 pt-3 text-left text-[13px] leading-[18px] outline-none focus-visible:ring-2 focus-visible:ring-sidebar-ring">
        <Avatar className="size-8">
          {user.pictureUrl ? <AvatarImage src={user.pictureUrl} alt="" /> : null}
          <AvatarFallback className="bg-sidebar-foreground font-semibold text-graphite">
            {initials(user.name)}
          </AvatarFallback>
        </Avatar>
        <span className="min-w-0">
          <span className="block truncate">{user.name}</span>
          <span className="block opacity-70">Signed in with Google</span>
        </span>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="start" side="top" className="w-56">
        <DropdownMenuItem onSelect={toggle}>
          {theme === 'dark' ? <Sun className="size-4" /> : <Moon className="size-4" />}
          {theme === 'dark' ? 'Light theme' : 'Dark theme'}
        </DropdownMenuItem>
        <DropdownMenuItem onSelect={() => void signOut()}>
          <LogOut className="size-4" />
          Sign out
        </DropdownMenuItem>
      </DropdownMenuContent>
    </DropdownMenu>
  )
}
